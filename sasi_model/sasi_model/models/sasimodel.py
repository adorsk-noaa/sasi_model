import sasi_model.conf as conf
from sasi_model.models import Result
import sys

class SASIModel(object):

    def __init__(self, dao=None, t0=0, tf=10, dt=1, taus=None, omegas=None):
        # Data access object.
        self.dao = dao

        # Start time.
        self.t0 = t0

        # End time.
        self.tf = tf

        # Timestep.
        self.dt = dt

        # tau (stochastic modifier for recovery)
        if not taus:
            taus = {
                    '0' : 1,
                    '1' : 2,
                    '2' : 5,
                    '3' : 10
                    }
        self.taus = taus

        # omega (stochastic modifier for damage)
        if not omegas:
            omegas = {
                    '0' : .10,
                    '1' : .25,
                    '2' : .50,
                    '3' : 1
                    }
        self.omegas = omegas

        # Results, grouped by time and cell.
        self.results_t_c = {}

        # Results, as a list.
        self.results = []

        self.setup()

    def setup(self):
        """
        This method creates lookups which can speed up model runs.
        """

        # Get habitat types, grouped by gears that can be applied to those
        # habitat types. 
        if conf.conf['verbose']: print >> sys.stderr, "Getting habitats by gear categories..."
        self.ht_by_g = {} 
        for row in self.dao.query({
            'SELECT': [
                {'ID': 's', 'EXPRESSION': '{{VA.substrate_id}}' },
                {'ID': 'e', 'EXPRESSION': '{{VA.energy}}' },
                {'ID': 'g', 'EXPRESSION': '{{VA.gear_id}}'} 
            ],
            'GROUP_BY': [{'ID': 's'}, {'ID': 'e'}, {'ID': 'g'}]
        }):
            hts = self.ht_by_g.setdefault(row.g, [])
            hts.append((row.s, row.e,))

        # Get feature codes, grouped by gear categories that can be applied to those
        # feature types.
        if conf.conf['verbose']: print >> sys.stderr, "Getting features by gear categories..."
        self.f_by_g = {}
        for row in self.dao.query({
            'SELECT': [
                {'ID': 'f', 'EXPRESSION': '{{VA.feature_id}}'},
                {'ID': 'g', 'EXPRESSION': '{{VA.gear_id}}' }
            ],
            'GROUP_BY': [{'ID': 'f'}, {'ID': 'g'}]
        }):
            fs = self.f_by_g.setdefault(row.g, [])
            fs.append(row.f)

        # Get features grouped by category and habitat types.
        if conf.conf['verbose']: print >> sys.stderr, "Getting features by gear categories..."
        self.f_by_ht_fc = {}
        for row in self.dao.query({
            'SELECT': [
                {'ID': 'f', 'EXPRESSION': '{{VA.feature_id}}'},
                {'ID': 'fc', 'EXPRESSION': '{{Feature.category}}'},
                {'ID': 's', 'EXPRESSION': '{{VA.substrate_id}}' }, 
                {'ID': 'e', 'EXPRESSION': '{{VA.energy}}' },
            ],
            'FROM': [
                {'SOURCE': 'VA', 'JOINS': [
                    ['Feature', [
                        {'TYPE': 'ENTITY', 
                         'EXPRESSION': '{{VA.feature_id}}'},
                        '==',
                        {'TYPE': 'ENTITY', 
                         'EXPRESSION': '{{Feature.id}}'},
                    ]]
                ]}
            ],
            'GROUP_BY': [{'ID': 'f'}, {'ID': 'fc'}, {'ID': 's'}, {'ID': 'e'}]
        }):
            ht = (row.s, row.e,)
            ht_fcs = self.f_by_ht_fc.setdefault(ht, {})
            fc = ht_fcs.setdefault(row.fc, [])
            fc.append(row.f)

        # Create feature lookup.
        if conf.conf['verbose']: print >> sys.stderr, "Creating features lookup..."
        self.features = {}
        for f in self.dao.query('{{Feature}}'):
            self.features[f.id] = f

        # Create cells-habitat_type-feature_category-feature lookup.
        if conf.conf['verbose']: print >> sys.stderr, "Creating cells-habitat_type-feature lookup..."
        self.c_ht_fc_f = self.get_c_ht_fc_f_lookup()

        # Create effort lookup by cell and time.
        if conf.conf['verbose']: print >> sys.stderr, "Creating cells-time-effort lookup..."
        self.c_t_e = self.get_c_t_e_lookup()

        # Create va lookup.
        self.vas = {}
        for va in self.dao.query('{{VA}}'):
            key = (va.gear_id, va.substrate_id, va.energy, va.feature_id)
            self.vas[key] = va

        # Initialize results, grouped by time and cell.
        if conf.conf['verbose']: print >> sys.stderr, "Initializing results..."
        for t in range(self.t0, self.tf + self.dt, self.dt):
            self.results_t_c[t] = {}
            for c in self.c_ht_fc_f.keys():
                self.results_t_c[t][c] = {}

    def get_c_ht_fc_f_lookup(self):

        # Initialize cell-habitat_type-feature lookup.
        c_ht_fc_f = {}

        # For each cell...
        for c in self.dao.query('{{Cell}}'):

            # Create entry in c_ht_f for cell.
            c_ht_fc_f[c.id] = {
                'ht': {}
            }

            # For each habitat type in the cell's habitat composition...
            for ht, pct_area in c.habitat_composition.items():

                # Create entry for ht in c_ht_f.
                c_ht_fc_f[c.id]['ht'][ht] = {}

                # Save percent area.
                c_ht_fc_f[c.id]['ht'][ht]['percent_cell_area'] = pct_area

                # Calculate habitat type area.
                c_ht_fc_f[c.id]['ht'][ht]['area'] = c.area * pct_area

                # Get features for habitat, grouped by featured category.
                c_ht_fc_f[c.id]['ht'][ht]['fc'] = {}
                for feature_category, feature_ids in self.f_by_ht_fc[ht].items():
                    features = {}
                    for f_id in feature_ids:
                        features[f_id] = self.features[f_id]
                    c_ht_fc_f[c.id]['ht'][ht]['fc'][feature_category] = features

        return c_ht_fc_f

    def get_c_t_e_lookup(self):

        # Initialize lookup.
        c_t_e = {}

        # For each effort in the model's time range...
        effort_counter = 0
        for e in self.dao.query({
            'SELECT': '{{Effort}}',
            'WHERE': [
                [{'TYPE': 'ENTITY', 'EXPRESSION': '{{Effort.time}}'},
                 '>=', self.t0],
                [{'TYPE': 'ENTITY', 'EXPRESSION': '{{Effort.time}}'},
                 '<=', self.tf],
            ]
        }):
            effort_counter += 1
            if conf.conf['verbose']: 
                if (effort_counter % 1000) == 0: print >> sys.stderr, "effort: %s" % effort_counter


            # Create cell-time key.
            c_t_key = (e.cell_id, e.time)

            # Initialize lookup entries for cell-time key, if not existing.
            es = c_t_e.setdefault(c_t_key, [])

            # Add effort to lookup.	
            es.append(e)

        return c_t_e


    def run(self):

        # Iterate from t0 to tf...
        for t in range(self.t0, self.tf + 1, self.dt):
            self.iterate(t)

        # Save results.
        self.dao.save_all(self.results)

    def iterate(self, t):
        if conf.conf['verbose']:
            print >> sys.stderr, "time: %s" % t

        result_counter = 0

        # For each cell...
        cell_counter = 0
        for c in self.c_ht_fc_f.keys():

            if conf.conf['verbose']:
                if (cell_counter % 100) == 0: 
                    print >> sys.stderr, "\tc: %s" % cell_counter

            cell_counter += 1

            # Get contact-adjusted fishing efforts for the cell.
            cell_efforts = self.c_t_e.get((c,t),[])

            # For each effort...
            for effort in cell_efforts:

                # Get cell's habitat types which are relevant to the effort.
                relevant_habitat_types = []
                for ht in self.c_ht_fc_f[c]['ht'].keys():
                    if ht in self.ht_by_g[effort.gear_id]: 
                        relevant_habitat_types.append(ht)

                # If there were relevant habitat types...
                if relevant_habitat_types:

                    # Calculate the total area of the relevant habitats.
                    relevant_habitats_area = sum(
                        [self.c_ht_fc_f[c]['ht'][ht]['area'] 
                         for ht in relevant_habitat_types])

                    # For each habitat type...
                    for ht in relevant_habitat_types:

                        # Distribute the effort's swept area proportionally 
                        # to the habitat type's area as a fraction of the 
                        # total relevant area.
                        ht_area = self.c_ht_fc_f[c]['ht'][ht]['area']
                        ht_swept_area = effort.swept_area * (
                            ht_area/relevant_habitats_area)

                        # Distribute swept area equally across feature categories.
                        fcs = self.c_ht_fc_f[c]['ht'][ht]['fc'].keys()
                        fc_swept_area = ht_swept_area/len(fcs)

                        # For each feature category...
                        for fc in fcs:

                            # Get the features for which the gear can be applied. 
                            relevant_features = []
                            for f in self.c_ht_fc_f[c]['ht'][ht]['fc'][fc].values():
                                if f.id in self.f_by_g[effort.gear_id]: 
                                    relevant_features.append(f)

                            # If there were relevant features...
                            if relevant_features:

                                # Distribute the category's effort equally over the features.
                                f_swept_area = fc_swept_area/len(relevant_features)

                                # For each feature...
                                for f in relevant_features:

                                    # Get vulnerability assessment for the effort.
                                    va = self.vas[(effort.gear_id, ht[0], ht[1], f.id)]

                                    # Get modifiers.
                                    omega = self.omegas[va.s]
                                    tau = self.taus[va.r]

                                    # Get or create the result corresponding to the
                                    # current set of parameters.
                                    result_key = (ht[0], ht[1], effort.gear_id, f.id)
                                    result = self.get_or_create_result(t, c, result_key)

                                    # Add the resulting contact-adjusted
                                    # swept area to the a field.
                                    result.a += f_swept_area

                                    # Calculate adverse effect swept area and add to y field.
                                    adverse_effect_swept_area = f_swept_area * omega
                                    result.y += adverse_effect_swept_area

                                    # Calculate recovery per timestep.
                                    recovery_per_dt = adverse_effect_swept_area/tau

                                    # Add recovery to x field for future entries.
                                    for future_t in range(t + 1, t + tau + 1, self.dt):
                                        if future_t <= self.tf:
                                            future_result = self.get_or_create_result(future_t, c, result_key)
                                            future_result.x += recovery_per_dt

                                    # Calculate Z.
                                    result.z = result.x - result.y

                                    # Update znet
                                    result.znet += result.z

                                    # End of iteration.

    def get_or_create_result(self, t, cell_id, result_key):
        substrate_id = result_key[0]
        energy_id = result_key[1]
        gear_id = result_key[2]
        feature_id = result_key[3]
        # If result for key does not exist yet, create it and
        # add it to the lookup and list.
        if not self.results_t_c[t][cell_id].has_key(result_key):
            new_result = Result(
                    t=t,
                    cell_id=cell_id,
                    gear_id=gear_id,
                    substrate_id=substrate_id,
                    energy_id=energy_id,
                    feature_id=feature_id,
                    a=0.0,
                    x=0.0,
                    y=0.0,
                    z=0.0,
                    znet=0.0
                    )
            self.results_t_c[t][cell_id][result_key] = new_result
            self.results.append(new_result)

        return self.results_t_c[t][cell_id][result_key]
