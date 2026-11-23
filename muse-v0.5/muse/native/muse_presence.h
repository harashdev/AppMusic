#pragma once
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
int muse_pres_start(uint64_t app_id, const char* token);
void muse_pres_update(const char* title, const char* artist, const char* image, int64_t start_s, int64_t end_s);
void muse_pres_page(const char* label);
void muse_pres_clear(void);
void muse_pres_stop(void);
int muse_pres_status(void);
const char* muse_pres_error(void);
#ifdef __cplusplus
}
#endif
