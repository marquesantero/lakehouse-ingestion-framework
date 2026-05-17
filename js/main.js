document.addEventListener("DOMContentLoaded", () => {
  const sidebar = document.querySelector("[data-sidebar]");
  const toggle = document.querySelector("[data-nav-toggle]");

  toggle?.addEventListener("click", () => {
    sidebar?.classList.toggle("open");
  });

  document.querySelectorAll(".side-nav a").forEach((link) => {
    link.addEventListener("click", () => sidebar?.classList.remove("open"));
  });

  const page = document.body.dataset.page;
  if (page) {
    document.querySelectorAll(`[data-nav="${page}"]`).forEach((link) => {
      link.classList.add("active");
    });
  }

  document.querySelectorAll("pre").forEach((pre) => {
    if (pre.querySelector(".copy-btn")) return;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "copy-btn";
    button.textContent = "Copy";
    button.addEventListener("click", async () => {
      const text = pre.querySelector("code")?.innerText || pre.innerText;
      try {
        await navigator.clipboard.writeText(text);
        button.textContent = "Copied";
        button.classList.add("copied");
      } catch {
        button.textContent = "Failed";
      }
      window.setTimeout(() => {
        button.textContent = "Copy";
        button.classList.remove("copied");
      }, 1400);
    });
    pre.appendChild(button);
  });

  document.querySelectorAll("pre code").forEach((code) => {
    const text = code.textContent.trim();
    if (!code.className) {
      if (/^(source:|target:|mode:|preset:)/m.test(text)) code.classList.add("language-yaml");
      else if (/^(SELECT|WITH|MERGE|CREATE|ALTER|DELETE)\b/im.test(text)) code.classList.add("language-sql");
      else if (/^(pip|contractforge|databricks|python)\b/im.test(text)) code.classList.add("language-bash");
      else if (/(from contractforge|import |def |result =)/.test(text)) code.classList.add("language-python");
      else if (/^[\[{]/.test(text)) code.classList.add("language-json");
    }
  });

  if (window.hljs) window.hljs.highlightAll();

  if (window.mermaid) {
    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: "loose",
      theme: "base",
      themeVariables: {
        background: "#ffffff",
        primaryColor: "#ffffff",
        primaryTextColor: "#102235",
        primaryBorderColor: "#ded9cf",
        lineColor: "#b9792a",
        secondaryColor: "#f2f0eb",
        tertiaryColor: "#fff7ec",
        fontFamily: "IBM Plex Sans",
      },
      flowchart: { curve: "basis", htmlLabels: true },
    });
    window.mermaid.run({ querySelector: ".mermaid" }).catch((error) => {
      console.warn("Mermaid rendering failed", error);
    });
  }
});
