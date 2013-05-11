#ifndef _INCLUDED_FORCE_H
#define _INCLUDED_FORCE_H

typedef struct _force_params_t {
    bool do_close_repulsion;
    bool use_ref_freq;
    double anti_gravity_strength;
    double link_strength;
} force_params_t;

typedef struct _paper_t paper_t;
typedef struct _layout_t layout_t;
typedef struct _quad_tree_t quad_tree_t;
typedef struct _oct_tree_t oct_tree_t;

void quad_tree_forces(force_params_t *param, quad_tree_t *qt);
void compute_attractive_link_force_2d(force_params_t *param, bool do_tred, int num_papers, paper_t **papers);
void compute_attractive_link_force_2d_layout_version(force_params_t *param, bool do_tred, layout_t *layout);

void oct_tree_forces(force_params_t *param, oct_tree_t *ot);
void compute_attractive_link_force_3d(force_params_t *param, bool do_tred, int num_papers, paper_t **papers);

#endif // _INCLUDED_FORCE_H