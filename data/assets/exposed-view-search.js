(function () {
  'use strict';

  var views = document.querySelectorAll(
    '[data-htmlify-exposed-view-version]'
  );
  if (!views.length) return;

  function clean(value) {
    return String(value || '').trim().toLocaleLowerCase();
  }

  function labelFor(control) {
    var id = control.getAttribute('id');
    var label = id && document.querySelector('label[for="' + id + '"]');
    return clean(label && label.textContent).replace(/:$/, '');
  }

  function fieldCandidates(name) {
    var candidates = [name];
    ['_target_id', '_value', '_tid', '_list', '_min', '_max'].forEach(
      function (suffix) {
        if (name.slice(-suffix.length) === suffix) {
          candidates.push(name.slice(0, -suffix.length));
        }
      }
    );
    return candidates.map(function (value) {
      return value.replace(/_/g, '-').replace(/[^a-zA-Z0-9-]/g, '');
    });
  }

  function textForControl(row, control) {
    var name = control.name || '';
    if (!name || name === 'combine') return clean(row.textContent);

    var candidates = fieldCandidates(name);
    for (var index = 0; index < candidates.length; index += 1) {
      var field = row.querySelector('.views-field-' + candidates[index]);
      if (field) return clean(field.textContent);
    }

    var wantedLabel = labelFor(control);
    if (wantedLabel) {
      var fields = row.querySelectorAll('.views-field');
      for (var fieldIndex = 0; fieldIndex < fields.length; fieldIndex += 1) {
        var visibleLabel = fields[fieldIndex].querySelector('.views-label');
        var fieldLabel = clean(visibleLabel && visibleLabel.textContent)
          .replace(/:$/, '');
        if (fieldLabel === wantedLabel) return clean(fields[fieldIndex].textContent);
      }
    }
    return clean(row.textContent);
  }

  function selectedText(control) {
    if (control.tagName === 'SELECT') {
      var option = control.options[control.selectedIndex];
      if (!option || !control.value || control.value === 'All') return '';
      return clean(option.textContent).replace(/^[-\s]+|[-\s]+$/g, '');
    }
    if ((control.type === 'checkbox' || control.type === 'radio') &&
        !control.checked) return '';
    return clean(control.value);
  }

  function enhance(view, viewIndex) {
    var form = view.querySelector('.htmlify-exposed-view-search');
    var rows = Array.prototype.slice.call(
      view.querySelectorAll('.htmlify-exposed-view-row')
    );
    var status = view.querySelector('.htmlify-exposed-view-status');
    var pagination = view.querySelector('.htmlify-exposed-view-pagination');
    var clear = view.querySelector('.htmlify-exposed-view-clear');
    var pageSize = parseInt(view.getAttribute('data-htmlify-page-size'), 10) || 50;
    var pageParameter = views.length === 1
      ? 'view_page'
      : 'view_' + (viewIndex + 1) + '_page';
    var state = {page: 1};

    if (!form || !status || !pagination) return;
    view.classList.add('htmlify-exposed-view--enhanced');

    function controls() {
      return Array.prototype.slice.call(form.elements).filter(function (control) {
        return control.name &&
          control.type !== 'submit' &&
          control.type !== 'button' &&
          control.type !== 'hidden';
      });
    }

    function readState() {
      var params = new URLSearchParams(window.location.search);
      controls().forEach(function (control) {
        if (!params.has(control.name)) return;
        if (control.type === 'checkbox' || control.type === 'radio') {
          control.checked = params.getAll(control.name).indexOf(control.value) !== -1;
        } else {
          control.value = params.get(control.name);
        }
      });
      state.page = Math.max(1, parseInt(params.get(pageParameter), 10) || 1);
    }

    function writeState(push) {
      var params = new URLSearchParams(window.location.search);
      controls().forEach(function (control) {
        params.delete(control.name);
      });
      controls().forEach(function (control) {
        var value = selectedText(control);
        if (value) params.append(control.name, control.value);
      });
      params.delete(pageParameter);
      if (state.page > 1) params.set(pageParameter, String(state.page));
      var query = params.toString();
      var url = window.location.pathname + (query ? '?' + query : '');
      window.history[push ? 'pushState' : 'replaceState']({}, '', url);
    }

    function matches(row) {
      return controls().every(function (control) {
        var query = selectedText(control);
        if (!query) return true;
        var terms = query.split(/\s+/).filter(Boolean);
        var haystack = textForControl(row, control);
        return terms.every(function (term) {
          return haystack.indexOf(term) !== -1;
        });
      });
    }

    function renderPagination(totalPages) {
      pagination.textContent = '';
      if (totalPages <= 1) return;

      function button(label, page, disabled) {
        var element = document.createElement('button');
        element.type = 'button';
        element.textContent = label;
        element.disabled = disabled;
        element.addEventListener('click', function () {
          state.page = page;
          writeState(true);
          render();
          view.scrollIntoView({behavior: 'smooth'});
        });
        pagination.appendChild(element);
      }

      button('Previous', state.page - 1, state.page <= 1);
      var label = document.createElement('span');
      label.textContent = 'Page ' + state.page + ' of ' + totalPages;
      pagination.appendChild(label);
      button('Next', state.page + 1, state.page >= totalPages);
    }

    function render() {
      var matching = rows.filter(matches);
      var totalPages = Math.max(1, Math.ceil(matching.length / pageSize));
      state.page = Math.min(state.page, totalPages);
      var start = (state.page - 1) * pageSize;
      var end = Math.min(start + pageSize, matching.length);
      rows.forEach(function (row) { row.hidden = true; });
      matching.slice(start, end).forEach(function (row) { row.hidden = false; });
      status.textContent = matching.length
        ? matching.length + ' matching records. Showing ' + (start + 1) + '–' + end + '.'
        : 'No matching records found.';
      renderPagination(totalPages);
    }

    form.addEventListener('submit', function (event) {
      event.preventDefault();
      state.page = 1;
      writeState(true);
      render();
    });
    form.addEventListener('change', function () {
      state.page = 1;
      writeState(true);
      render();
    });
    if (clear) clear.addEventListener('click', function () {
      form.reset();
      controls().forEach(function (control) {
        if (control.type === 'checkbox' || control.type === 'radio') {
          control.checked = false;
        } else if (control.tagName !== 'SELECT') {
          control.value = '';
        }
      });
      state.page = 1;
      writeState(true);
      render();
    });
    window.addEventListener('popstate', function () {
      form.reset();
      readState();
      render();
    });

    readState();
    render();
  }

  Array.prototype.forEach.call(views, enhance);
}());
