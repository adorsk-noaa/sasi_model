from sasi_data.models import Result
import sys


class SASI_Model(object):

    def __init__(self, t0=0, tf=10, dt=1, taus=None, omegas=None,
                 cells=None, features=None, efforts=None, vas=None,
                 opts={}):
        self.t0 = t0 # Start time
        self.tf = tf # End time
        self.dt = dt # Time step
        # tau (recovery modifier)
        if not taus:
            taus = {
                    '0' : 1,
                    '1' : 2,
                    '2' : 5,
                    '3' : 10
                    }
        self.taus = taus

        # omega (damage modifier)
        if not omegas:
            omegas = {
                    '0' : .10,
                    '1' : .25,
                    '2' : .50,
                    '3' : 1
                    }
        self.omegas = omegas

        self.cells = cells
        self.features = features
        self.efforts = efforts
        self.vas = vas # Vulnerability Assessments

        # Other options e.g. 'verbose'.
        self.opts = opts

        # Results, grouped by time and cell.
        self.results_t_c = {}

        # Results, as a list.
        self.results = []

        self.setup()

    def setup(self):
        """
        Create look-ups.
        """

        # Create va lookup.
        self.va_lu = {}
        for va in self.vas:
            key = (va.gear_id, va.substrate_id, va.energy_id, va.feature_id)
            self.va_lu[key] = va

        # Group habitat types by gears to which they can be applied.
        if self.opts.get('verbose'): 
            print >> sys.stderr, "Grouping habitats by gears..."
        self.ht_by_g = {} 
        for va in self.vas:
            ht = (va.substrate_id, va.energy_id,)
            gear_hts = self.ht_by_g.setdefault(va.gear_id, set())
            gear_hts.add(ht)

        # Group features by gears to which they can be applied.
        if self.opts.get('verbose'): 
            print >> sys.stderr, "Grouping features by gears..."
        self.f_by_g = {}
        for va in self.vas:
            gear_fs = self.f_by_g.setdefault(va.gear_id, set())
            gear_fs.add(va.feature_id)

        # Create feature lookup.
        if self.opts.get('verbose'): 
            print >> sys.stderr, "Creating features lookup..."
        self.features_lu = {}
        for f in self.features:
            self.features_lu[f.id] = f

        # Group features by category and habitat types.
        if self.opts.get('verbose'): 
            print >> sys.stderr, ("Grouping features by categories and"
                                  " habitats...")
        self.f_by_ht_fc = {}
        for va in self.vas:
            ht = (va.substrate_id, va.energy_id,)
            ht_fcs = self.f_by_ht_fc.setdefault(ht, {})
            f = self.features_lu[va.feature_id]
            fc = f.category
            fc_fs = ht_fcs.setdefault(fc, set())
            fc_fs.add(va.feature_id)


        # Create cells-habitat_type-feature_category-feature lookup.
        if self.opts.get('verbose'): 
            print >> sys.stderr, ("Creating cells-habitat_type-"
                                  "feature_category-feature lookup...")
        self.c_ht_fc_f = self.get_c_ht_fc_f_lookup()

        # Create effort lookup by cell and time.
        if self.opts.get('verbose'): 
            print >> sys.stderr, "Creating cells-time-effort lookup..."
        self.c_t_e = self.get_c_t_e_lookup()

        # Initialize results, grouped by time and cell.
        if self.opts.get('verbose'): 
            print >> sys.stderr, "Initializing results..."
        for t in range(self.t0, self.tf + self.dt, self.dt):
            self.results_t_c[t] = {}
            for c in self.c_ht_fc_f.keys():
                self.results_t_c[t][c] = {}

    def get_c_ht_fc_f_lookup(self):
        """ Create cells-habitat_type-feature_category-feature lookup. """
        c_ht_fc_f = {}
        for c in self.cells:
            c_ht_fc_f[c.id] = { 'ht': {} }
            for ht, pct_area in c.habitat_composition.items():
                c_ht_fc_f[c.id]['ht'][ht] = {
                    'area': pct_area * c.area,
                    'fc': {}
                }
                for fc, fs in self.f_by_ht_fc[ht].items():
                    c_ht_fc_f[c.id]['ht'][ht]['fc'][fc] = fs
        return c_ht_fc_f

    def get_c_t_e_lookup(self):
        """ Create cells-time-effort lookup. """
        c_t_e = {}
        effort_counter = 0
        for e in self.efforts:
            effort_counter += 1
            if self.opts.get('verbose'): 
                if (effort_counter % 1000) == 0: 
                    print >> sys.stderr, "effort: %s" % effort_counter
            c_t = (e.cell_id, e.time,)
            es = c_t_e.setdefault(c_t, [])
            es.append(e)
        return c_t_e

    def run(self):
        for t in range(self.t0, self.tf + 1, self.dt):
            self.iterate(t)

    def iterate(self, t):
        if self.opts.get('verbose'):
            print >> sys.stderr, "time: %s" % t

        result_counter = 0

        cell_counter = 0
        for c in self.c_ht_fc_f.keys():

            if self.opts.get('verbose'):
                if (cell_counter % 100) == 0: 
                    print >> sys.stderr, "\tc: %s" % cell_counter
            cell_counter += 1

            cell_efforts = self.c_t_e.get((c,t),[])

            for effort in cell_efforts:

                relevant_habitat_types = []
                for ht in self.c_ht_fc_f[c]['ht'].keys():
                    if ht in self.ht_by_g[effort.gear_id]: 
                        relevant_habitat_types.append(ht)

                if relevant_habitat_types:

                    relevant_habitats_area = sum(
                        [self.c_ht_fc_f[c]['ht'][ht]['area'] 
                         for ht in relevant_habitat_types])

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

                        for fc in fcs:

                            relevant_features = []
                            for f in self.c_ht_fc_f[c]['ht'][ht]['fc'][fc]:
                                if f in self.f_by_g[effort.gear_id]: 
                                    relevant_features.append(f)

                            if relevant_features:

                                # Distribute the category's effort equally over the features.
                                f_swept_area = fc_swept_area/len(relevant_features)

                                for f in relevant_features:

                                    # Get vulnerability assessment for the effort.
                                    va = self.va_lu[(effort.gear_id, ht[0], ht[1], f)]

                                    omega = self.omegas[va.s]
                                    tau = self.taus[va.r]

                                    result_key = (ht[0], ht[1], effort.gear_id, f)
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