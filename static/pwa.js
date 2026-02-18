(() => {
  if (!("serviceWorker" in navigator)) return;
  let refreshed = false;
  navigator.serviceWorker.addEventListener("controllerchange", () => {
    if (refreshed) return;
    refreshed = true;
    window.location.reload();
  });
  window.addEventListener("load", () => {
    navigator.serviceWorker
      .register("/static/sw.js")
      .then((reg) => reg.update())
      .catch(() => {});
  });
})();
