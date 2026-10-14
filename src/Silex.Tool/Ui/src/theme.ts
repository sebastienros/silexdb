(() => {
  const param = new URLSearchParams(window.location.search).get("clawpilotTheme");
  const theme = param || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  document.documentElement.setAttribute("data-theme", theme);
})();
