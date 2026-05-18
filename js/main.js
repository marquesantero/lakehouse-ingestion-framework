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
  const currentPath = window.location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll(".side-nav a").forEach((link) => {
    const href = link.getAttribute("href") || "";
    if (href.endsWith(currentPath) && currentPath !== "index.html") {
      link.classList.add("active");
    }
  });

  document.querySelectorAll(".side-nav").forEach((nav) => {
    const children = Array.from(nav.children);
    let currentGroup = null;

    children.forEach((child) => {
      if (child.classList.contains("nav-label")) {
        currentGroup = document.createElement("section");
        currentGroup.className = "nav-group";

        const button = document.createElement("button");
        button.type = "button";
        button.className = "nav-group-toggle";
        button.setAttribute("aria-expanded", "false");
        button.textContent = child.textContent;

        const links = document.createElement("div");
        links.className = "nav-group-links";

        currentGroup.append(button, links);
        nav.insertBefore(currentGroup, child);
        child.remove();
        return;
      }

      if (currentGroup) {
        currentGroup.querySelector(".nav-group-links")?.appendChild(child);
      }
    });

    nav.querySelectorAll(".nav-group").forEach((group) => {
      const button = group.querySelector(".nav-group-toggle");
      const links = group.querySelector(".nav-group-links");
      const hasActive = Boolean(group.querySelector("a.active"));
      const isConnectorsPage = page === "connectors" && links?.querySelector('a[href*="connectors"], a[href="index.html"]');
      const shouldOpen = hasActive || isConnectorsPage || group === nav.querySelector(".nav-group");

      group.classList.toggle("open", shouldOpen);
      button?.setAttribute("aria-expanded", String(shouldOpen));

      button?.addEventListener("click", () => {
        const nextState = !group.classList.contains("open");
        group.classList.toggle("open", nextState);
        button.setAttribute("aria-expanded", String(nextState));
      });
    });
  });

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
