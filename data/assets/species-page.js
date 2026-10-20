(function () {
  'use strict';

  function initialiseTabs(root) {
    var links = Array.prototype.slice.call(
      root.querySelectorAll('.htmlify-species-tabs a[data-species-tab]')
    );
    var panels = Array.prototype.slice.call(
      root.querySelectorAll('.htmlify-species-panel[data-species-tab]')
    );
    if (!links.length || !panels.length) return;

    function select(tab, updateHistory) {
      var selected = panels.some(function (panel) {
        return panel.getAttribute('data-species-tab') === tab;
      }) ? tab : panels[0].getAttribute('data-species-tab');

      links.forEach(function (link) {
        var active = link.getAttribute('data-species-tab') === selected;
        link.classList.toggle('active', active);
        link.setAttribute('aria-selected', active ? 'true' : 'false');
        link.setAttribute('tabindex', active ? '0' : '-1');
      });
      panels.forEach(function (panel) {
        var active = panel.getAttribute('data-species-tab') === selected;
        panel.hidden = !active;
      });

      if (updateHistory) {
        window.history.pushState({}, '', '#species-tab-' + selected);
      }
    }

    links.forEach(function (link) {
      link.addEventListener('click', function (event) {
        event.preventDefault();
        select(link.getAttribute('data-species-tab'), true);
      });
    });
    window.addEventListener('popstate', function () {
      select((window.location.hash || '').replace('#species-tab-', ''), false);
    });

    root.classList.add('htmlify-species-page--enhanced');
    select((window.location.hash || '').replace('#species-tab-', ''), false);
  }

  function initialiseTree(root) {
    var status = root.querySelector('.htmlify-taxonomy-status');
    var container = root.querySelector('.htmlify-taxonomy-list');
    var current = String(root.getAttribute('data-current-tid') || '');

    function render(data) {
      var children = {};
      var parents = {};
      data.nodes.forEach(function (node) {
        var parent = String(node.parent || 0);
        if (!children[parent]) children[parent] = [];
        children[parent].push(node);
        if (!parents[String(node.id)]) parents[String(node.id)] = [];
        parents[String(node.id)].push(parent);
      });

      var ancestors = {};
      var pending = (parents[current] || []).slice();
      while (pending.length) {
        var parent = pending.pop();
        if (parent === '0' || ancestors[parent]) continue;
        ancestors[parent] = true;
        pending = pending.concat(parents[parent] || []);
      }

      function branch(parent, lineage) {
        var list = document.createElement('ul');
        (children[parent] || []).forEach(function (node) {
          var id = String(node.id);
          if (lineage[id]) return;
          var item = document.createElement('li');
          var link = document.createElement('a');
          link.href = '/taxonomy/term/' + id;
          link.textContent = node.name;
          if (id === current) {
            link.className = 'active';
            link.setAttribute('aria-current', 'page');
          }

          var descendants = children[id] || [];
          if (descendants.length) {
            var details = document.createElement('details');
            details.open = id === current || !!ancestors[id];
            var summary = document.createElement('summary');
            summary.appendChild(link);
            details.appendChild(summary);
            var nextLineage = Object.assign({}, lineage);
            nextLineage[id] = true;
            details.appendChild(branch(id, nextLineage));
            item.appendChild(details);
          } else {
            item.appendChild(link);
          }
          list.appendChild(item);
        });
        return list;
      }

      container.textContent = '';
      container.appendChild(branch('0', {}));
      status.remove();
    }

    fetch(root.getAttribute('data-index-url'))
      .then(function (response) {
        if (!response.ok) throw new Error('HTTP ' + response.status);
        return response.json();
      })
      .then(render)
      .catch(function () {
        status.textContent = 'The taxonomy tree could not be loaded.';
      });
  }

  document.querySelectorAll('.htmlify-species-page').forEach(initialiseTabs);
  document.querySelectorAll('.htmlify-taxonomy-tree').forEach(initialiseTree);
}());
