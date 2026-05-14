/* =====================================================
   ContractForge — Interatividade
   Navegação, tabs, copy, busca, scroll, mobile menu
   ===================================================== */

document.addEventListener('DOMContentLoaded', () => {

  /* ---- Mobile menu ---- */
  const toggle = document.getElementById('nav-toggle');
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebar-overlay');

  toggle?.addEventListener('click', () => {
    sidebar.classList.toggle('show');
    overlay.classList.toggle('show');
  });
  overlay?.addEventListener('click', () => {
    sidebar.classList.remove('show');
    overlay.classList.remove('show');
  });
  document.querySelectorAll('.sidebar-nav a').forEach(a => {
    a.addEventListener('click', () => {
      sidebar.classList.remove('show');
      overlay.classList.remove('show');
    });
  });

  /* ---- Highlight active nav ---- */
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.sidebar-nav a[href^="#"]');

  function updateActive() {
    let current = '';
    sections.forEach(s => {
      const top = s.getBoundingClientRect().top;
      if (top < 120) current = s.id;
    });
    navLinks.forEach(a => {
      const active = a.getAttribute('href') === '#' + current;
      a.classList.toggle('active', active);
      if (active) a.closest('details')?.setAttribute('open', '');
    });
  }
  window.addEventListener('scroll', updateActive, { passive: true });

  /* ---- Back to top ---- */
  const backBtn = document.getElementById('back-top');
  window.addEventListener('scroll', () => {
    backBtn?.classList.toggle('visible', window.scrollY > 600);
  }, { passive: true });
  backBtn?.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));

  /* ---- Tabs ---- */
  function tabContentsFor(tabGroup) {
    const contents = [];
    let node = tabGroup.nextElementSibling;
    while (node) {
      if (node.classList?.contains('tabs')) break;
      if (node.tagName === 'SECTION') break;
      if (node.classList?.contains('tab-content')) contents.push(node);
      node = node.nextElementSibling;
    }
    return contents;
  }

  document.querySelectorAll('.tabs').forEach(tabGroup => {
    const buttons = tabGroup.querySelectorAll('.tab-btn');
    const contents = tabContentsFor(tabGroup);
    buttons.forEach(btn => {
      btn.addEventListener('click', () => {
        const lang = btn.dataset.lang;
        buttons.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        contents.forEach(c => c.classList.remove('active'));
        contents.find(c => c.dataset.lang === lang)?.classList.add('active');
      });
    });
  });

  /* ---- Syntax highlight ---- */
  function detectCodeLanguage(code) {
    const text = code.textContent.trim();
    const tab = code.closest('.tab-content')?.dataset.lang;
    if (tab) {
      if (tab.startsWith('yaml')) return 'yaml';
      if (['python', 'sql', 'bash', 'json'].includes(tab)) return tab;
    }
    if (/^(SELECT|WITH|MERGE|CREATE|ALTER|DELETE|VACUUM|OPTIMIZE)\b/im.test(text)) return 'sql';
    if (/^(pip|pytest|databricks|contractforge|git|python\s+-m)\b/im.test(text)) return 'bash';
    if (/^\s*[\[{]/.test(text) && /["}]/.test(text)) return 'json';
    if (/^\s*[A-Za-z0-9_.-]+:\s/m.test(text) && !/(def |import |from )/.test(text)) return 'yaml';
    if (/(from\s+lakehouse_ingestion|import\s+|def\s+|result\s*=|spark\.)/.test(text)) return 'python';
    return '';
  }

  document.querySelectorAll('pre code').forEach(code => {
    const lang = detectCodeLanguage(code);
    if (lang) code.classList.add(`language-${lang}`);
  });
  if (window.hljs) {
    window.hljs.highlightAll();
  }

  /* ---- Mermaid diagrams ---- */
  if (window.mermaid) {
    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'loose',
      theme: 'base',
      themeVariables: {
        background: '#ffffff',
        primaryColor: '#ffffff',
        primaryTextColor: '#111827',
        primaryBorderColor: '#d9dde3',
        lineColor: '#ff3621',
        secondaryColor: '#f5f6f8',
        tertiaryColor: '#eef1f5',
        noteBkgColor: '#f5f6f8',
        noteTextColor: '#1f2328',
        actorBkg: '#ffffff',
        actorBorder: '#ff3621',
        actorTextColor: '#111827',
        signalColor: '#1f2328',
        signalTextColor: '#1f2328',
      },
      flowchart: {
        curve: 'basis',
        htmlLabels: true,
      },
      sequence: {
        mirrorActors: false,
      },
    });
    window.mermaid.run({ querySelector: '.mermaid' }).catch(error => {
      console.warn('Falha ao renderizar Mermaid', error);
    });
  }

  /* ---- Expand diagrams ---- */
  function closeExpandedDiagram() {
    document.querySelectorAll('.diagram-card.expanded').forEach(card => {
      card.classList.remove('expanded');
      card.querySelector('.diagram-expand-btn')?.setAttribute('aria-expanded', 'false');
      const btn = card.querySelector('.diagram-expand-btn');
      if (btn) btn.textContent = 'Expandir';
    });
    document.body.classList.remove('diagram-open');
  }

  document.querySelectorAll('.diagram-card').forEach(card => {
    if (card.querySelector('.diagram-expand-btn')) return;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'diagram-expand-btn';
    btn.textContent = 'Expandir';
    btn.setAttribute('aria-expanded', 'false');
    btn.addEventListener('click', () => {
      const willExpand = !card.classList.contains('expanded');
      closeExpandedDiagram();
      if (willExpand) {
        card.classList.add('expanded');
        document.body.classList.add('diagram-open');
        btn.textContent = 'Fechar';
        btn.setAttribute('aria-expanded', 'true');
      }
    });
    card.prepend(btn);
  });

  document.addEventListener('keydown', event => {
    if (event.key === 'Escape') closeExpandedDiagram();
  });
  document.addEventListener('click', event => {
    if (event.target === document.body && document.body.classList.contains('diagram-open')) {
      closeExpandedDiagram();
    }
  });

  /* ---- Copy code ---- */
  document.querySelectorAll('pre').forEach(pre => {
    if (pre.querySelector('.copy-btn')) return;
    const btn = document.createElement('button');
    btn.className = 'copy-btn';
    btn.textContent = 'Copiar';
    btn.addEventListener('click', () => {
      const code = pre.querySelector('code')?.innerText || pre.innerText;
      const write = navigator.clipboard?.writeText
        ? navigator.clipboard.writeText(code)
        : Promise.reject(new Error('clipboard unavailable'));
      write.then(() => {
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => { btn.textContent = 'Copiar'; btn.classList.remove('copied'); }, 1800);
      }).catch(() => {
        btn.textContent = 'Falhou';
        setTimeout(() => { btn.textContent = 'Copiar'; }, 1200);
      });
    });
    pre.appendChild(btn);
  });

  /* ---- Search ---- */
  const searchInput = document.getElementById('search-nav');
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      const q = searchInput.value.toLowerCase().trim();
      navLinks.forEach(a => {
        const text = (a.textContent || '').toLowerCase();
        const parent = a.closest('li') || a;
        parent.style.display = !q || text.includes(q) ? '' : 'none';
        if (q && text.includes(q)) a.closest('details')?.setAttribute('open', '');
      });
      // show/hide group titles
      document.querySelectorAll('.nav-group-title').forEach(g => {
        const group = g.nextElementSibling;
        if (!group) return;
        const visible = group.querySelectorAll('a').length > 0 &&
          Array.from(group.querySelectorAll('a')).some(a => {
            const li = a.closest('li') || a;
            return li.style.display !== 'none';
          });
        g.style.display = q ? (visible ? '' : 'none') : '';
      });
    });
  }

  /* ---- Smooth scroll for anchor links ---- */
  document.querySelectorAll('a[href^="#"]').forEach(a => {
    a.addEventListener('click', e => {
      const id = a.getAttribute('href');
      if (id === '#') return;
      const target = document.querySelector(id);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
        history.pushState(null, '', id);
      }
    });
  });

});
