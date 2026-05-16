/**
 * @file web_ui.h
 * @brief Declarations for the embedded web management UI.
 *
 * The proxy's Prometheus HTTP port serves a small single-page application at
 * GET /ui and a JSON status endpoint at GET /api/status.json.
 *
 * keel_ui_html is the full HTML/CSS/JS for the SPA.  It is defined (not just
 * declared) in admin.c so it is co-located with the HTTP handler that serves
 * it.  Exporting it here allows unit tests to inspect its content without
 * linking the entire admin subsystem.
 */

#ifndef KEEL_WEB_UI_H
#define KEEL_WEB_UI_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Embedded single-page application HTML for the web management UI.
 *
 * Served verbatim in response to GET /ui requests on the Prometheus port.
 * The page auto-refreshes every 5 seconds by fetching /api/status.json.
 */
extern const char keel_ui_html[];

#ifdef __cplusplus
}
#endif

#endif /* KEEL_WEB_UI_H */
