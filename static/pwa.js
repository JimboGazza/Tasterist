(() => {
  if (!("serviceWorker" in navigator)) return;
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/static/sw.js").catch(() => {});
  });
})();
