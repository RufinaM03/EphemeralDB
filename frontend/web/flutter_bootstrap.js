{{flutter_js}}
{{flutter_build_config}}

// Serve the renderer from this deployment. Experiment consoles should not depend
// on an external CanvasKit CDN or retain old builds through offline caching.
_flutter.loader.load({config: {canvasKitBaseUrl: 'canvaskit/'}});
