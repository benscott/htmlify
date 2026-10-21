(function () {
  'use strict';

  var root = document.querySelector('.htmlify-facet-browser');
  if (!root) return;

  var controls = document.querySelector('.htmlify-facet-controls') || root;
  var form = controls.querySelector('.htmlify-facet-search');
  var queryInput = controls.querySelector('#htmlify-facet-query');
  var clearButton = controls.querySelector('.htmlify-facet-clear');
  var selectedElement = controls.querySelector('.htmlify-facet-selected');
  var groupsElement = controls.querySelector('.htmlify-facet-groups');
  var statusElement = root.querySelector('.htmlify-facet-status');
  var resultsElement = root.querySelector('.htmlify-facet-results');
  var paginationElement = root.querySelector('.htmlify-facet-pagination');
  var pageSize = 100;
  var valueLimit = 100;
  var data = null;
  var facetsByField = {};
  var state = {query: '', selected: {}, page: 1, open: {}};
  var facetFilters = {};

  function clean(value) {
    return String(value || '').trim().toLocaleLowerCase();
  }

  function initial(value) {
    var normalized = String(value || '').trim().normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
    var character = normalized.charAt(0).toUpperCase();
    return /^[A-Z]$/.test(character) ? character : '#';
  }

  function labelFor(field, value) {
    var facet = facetsByField[field];
    return facet && facet.values[String(value)] || String(value);
  }

  function readState() {
    var params = new URLSearchParams(window.location.search);
    state.query = params.get('q') || '';
    state.selected = {};
    params.getAll('facet').forEach(function (selection) {
      var separator = selection.indexOf(':');
      if (separator < 1) return;
      var field = selection.slice(0, separator);
      var value = selection.slice(separator + 1);
      if (!facetsByField[field] || !value) return;
      if (!state.selected[field]) state.selected[field] = [];
      if (state.selected[field].indexOf(value) === -1) {
        state.selected[field].push(value);
      }
      state.open[field] = true;
    });
    state.page = Math.max(1, parseInt(params.get('page') || '1', 10) || 1);
    queryInput.value = state.query;
  }

  function writeState(push) {
    var params = new URLSearchParams();
    if (state.query) params.set('q', state.query);
    Object.keys(state.selected).sort().forEach(function (field) {
      state.selected[field].forEach(function (value) {
        params.append('facet', field + ':' + value);
      });
    });
    if (state.page > 1) params.set('page', String(state.page));
    var url = window.location.pathname;
    if (params.toString()) url += '?' + params.toString();
    window.history[push ? 'pushState' : 'replaceState']({}, '', url);
  }

  function prepareData() {
    resultsElement.classList.toggle(
      'is-thumbnails',
      data.page && data.page.display === 'thumbnails'
    );
    data.facets.forEach(function (facet) {
      facetsByField[facet.field] = facet;
      facetFilters[facet.field] = {text: '', letter: ''};
    });
    data.records.forEach(function (record) {
      var parts = [record.title];
      Object.keys(record.facets || {}).forEach(function (field) {
        record.facets[field].forEach(function (value) {
          parts.push(labelFor(field, value));
        });
      });
      record._searchText = clean(parts.join(' '));
    });
  }

  function recordMatches(record, ignoredField) {
    if (state.query) {
      var terms = clean(state.query).split(/\s+/).filter(Boolean);
      if (!terms.every(function (term) {
        return record._searchText.indexOf(term) !== -1;
      })) return false;
    }
    return Object.keys(state.selected).every(function (field) {
      if (field === ignoredField) return true;
      var recordValues = record.facets[field] || [];
      return state.selected[field].every(function (value) {
        return recordValues.indexOf(value) !== -1;
      });
    });
  }

  function matchingRecords() {
    return data.records.filter(function (record) {
      return recordMatches(record, null);
    });
  }

  function countsFor(field) {
    var counts = {};
    data.records.forEach(function (record) {
      if (!recordMatches(record, field)) return;
      (record.facets[field] || []).forEach(function (value) {
        counts[value] = (counts[value] || 0) + 1;
      });
    });
    return counts;
  }

  function setSelection(field, value, selected) {
    var values = state.selected[field] || [];
    var position = values.indexOf(value);
    if (selected && position === -1) values.push(value);
    if (!selected && position !== -1) values.splice(position, 1);
    if (values.length) state.selected[field] = values;
    else delete state.selected[field];
    state.open[field] = true;
    state.page = 1;
    writeState(true);
    render();
  }

  function renderSelected() {
    selectedElement.textContent = '';
    var fields = Object.keys(state.selected);
    if (!fields.length) return;
    var label = document.createElement('strong');
    label.textContent = 'Selected filters: ';
    selectedElement.appendChild(label);
    fields.forEach(function (field) {
      state.selected[field].forEach(function (value) {
        var button = document.createElement('button');
        button.type = 'button';
        button.textContent = facetsByField[field].label + ': ' + labelFor(field, value) + ' ×';
        button.addEventListener('click', function () {
          setSelection(field, value, false);
        });
        selectedElement.appendChild(button);
      });
    });
  }

  function renderLetters(container, facet, entries, renderValues) {
    var available = {};
    entries.forEach(function (entry) {
      available[initial(entry.label)] = true;
    });
    var all = document.createElement('button');
    all.type = 'button';
    all.textContent = 'All';
    all.className = facetFilters[facet.field].letter ? '' : 'is-active';
    all.addEventListener('click', function () {
      facetFilters[facet.field].letter = '';
      renderValues();
    });
    container.appendChild(all);
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ#'.split('').forEach(function (letter) {
      var button = document.createElement('button');
      button.type = 'button';
      button.textContent = letter;
      button.disabled = !available[letter];
      button.className = facetFilters[facet.field].letter === letter ? 'is-active' : '';
      button.addEventListener('click', function () {
        facetFilters[facet.field].letter = letter;
        renderValues();
      });
      container.appendChild(button);
    });
  }

  function usesLetterFilter(facet) {
    return facet.field !== 'tus_biblio_year' &&
      facet.label.toLocaleLowerCase().indexOf('year') === -1;
  }

  function renderFacetGroup(facet) {
    var details = document.createElement('details');
    details.className = 'htmlify-facet-group';
    details.open = !!state.open[facet.field];
    details.addEventListener('toggle', function () {
      state.open[facet.field] = details.open;
    });
    var summary = document.createElement('summary');
    var selectedCount = (state.selected[facet.field] || []).length;
    summary.textContent = facet.label + (selectedCount ? ' (' + selectedCount + ' selected)' : '');
    details.appendChild(summary);

    var filter = document.createElement('input');
    filter.type = 'search';
    filter.className = 'htmlify-facet-value-search';
    filter.placeholder = 'Find ' + facet.label.toLocaleLowerCase();
    filter.setAttribute('aria-label', 'Find ' + facet.label.toLocaleLowerCase());
    filter.value = facetFilters[facet.field].text;
    details.appendChild(filter);

    var letters = document.createElement('nav');
    letters.className = 'htmlify-facet-letters';
    letters.setAttribute('aria-label', facet.label + ' initials');
    if (usesLetterFilter(facet)) details.appendChild(letters);

    var valuesElement = document.createElement('ul');
    valuesElement.className = 'htmlify-facet-values';
    details.appendChild(valuesElement);

    var more = document.createElement('p');
    more.className = 'htmlify-facet-more';
    details.appendChild(more);

    var counts = countsFor(facet.field);
    var entries = Object.keys(facet.values).map(function (value) {
      return {value: value, label: facet.values[value], count: counts[value] || 0};
    }).filter(function (entry) {
      return entry.count || (state.selected[facet.field] || []).indexOf(entry.value) !== -1;
    }).sort(function (left, right) {
      return left.label.localeCompare(right.label, undefined, {sensitivity: 'base'});
    });

    function renderValues() {
      valuesElement.textContent = '';
      letters.textContent = '';
      if (usesLetterFilter(facet)) {
        renderLetters(letters, facet, entries, renderValues);
      } else {
        facetFilters[facet.field].letter = '';
      }
      var textFilter = clean(facetFilters[facet.field].text);
      var letterFilter = facetFilters[facet.field].letter;
      var visible = entries.filter(function (entry) {
        return (!textFilter || clean(entry.label).indexOf(textFilter) !== -1) &&
          (!letterFilter || initial(entry.label) === letterFilter);
      });
      visible.slice(0, valueLimit).forEach(function (entry) {
        var item = document.createElement('li');
        var label = document.createElement('label');
        var checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = (state.selected[facet.field] || []).indexOf(entry.value) !== -1;
        checkbox.addEventListener('change', function () {
          setSelection(facet.field, entry.value, checkbox.checked);
        });
        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(' ' + entry.label + ' (' + entry.count + ')'));
        item.appendChild(label);
        valuesElement.appendChild(item);
      });
      more.textContent = visible.length > valueLimit
        ? 'Showing the first ' + valueLimit + ' of ' + visible.length + ' values. Narrow the list above.'
        : '';
    }

    filter.addEventListener('input', function () {
      facetFilters[facet.field].text = filter.value;
      facetFilters[facet.field].letter = '';
      renderValues();
    });
    renderValues();
    return details;
  }

  function renderFacets() {
    groupsElement.textContent = '';
    data.facets.forEach(function (facet) {
      groupsElement.appendChild(renderFacetGroup(facet));
    });
  }

  function renderPagination(totalPages) {
    paginationElement.textContent = '';
    if (totalPages <= 1) return;
    function addButton(label, page, disabled) {
      var button = document.createElement('button');
      button.type = 'button';
      button.textContent = label;
      button.disabled = disabled;
      button.addEventListener('click', function () {
        state.page = page;
        writeState(true);
        renderResults();
        root.scrollIntoView({behavior: 'smooth'});
      });
      paginationElement.appendChild(button);
    }
    addButton('Previous', state.page - 1, state.page <= 1);
    var label = document.createElement('span');
    label.textContent = 'Page ' + state.page + ' of ' + totalPages;
    paginationElement.appendChild(label);
    addButton('Next', state.page + 1, state.page >= totalPages);
  }

  function appendMetadata(item, record) {
    var parts = [];
    data.facets.forEach(function (facet) {
      var values = (record.facets[facet.field] || []).map(function (value) {
        return labelFor(facet.field, value);
      });
      if (values.length) {
        var visible = values.slice(0, 5).join(', ');
        if (values.length > 5) visible += ', …';
        parts.push(facet.label + ': ' + visible);
      }
    });
    if (!parts.length) return;
    var metadata = document.createElement('div');
    metadata.className = 'htmlify-facet-result-meta';
    metadata.textContent = parts.join(' · ');
    item.appendChild(metadata);
  }

  function renderResults() {
    resultsElement.textContent = '';
    var records = matchingRecords();
    var totalPages = Math.max(1, Math.ceil(records.length / pageSize));
    state.page = Math.min(state.page, totalPages);
    var offset = (state.page - 1) * pageSize;
    var visible = records.slice(offset, offset + pageSize);
    if (records.length) {
      statusElement.textContent = records.length + ' matching records. Showing ' +
        (offset + 1) + '–' + (offset + visible.length) + '.';
    } else {
      statusElement.textContent = 'No matching records found.';
    }
    visible.forEach(function (record) {
      var item = document.createElement('li');
      var thumbnailDisplay = data.page && data.page.display === 'thumbnails';
      if (record.thumbnail) {
        var imageLink = document.createElement('a');
        imageLink.href = record.url;
        imageLink.className = 'htmlify-facet-result-image';
        imageLink.setAttribute('aria-label', record.title);
        imageLink.title = record.title;
        var image = document.createElement('img');
        image.src = record.thumbnail;
        image.alt = record.title;
        image.width = 85;
        image.height = 85;
        image.loading = 'lazy';
        imageLink.appendChild(image);
        item.appendChild(imageLink);
      }
      if (!thumbnailDisplay || !record.thumbnail) {
        var title = document.createElement('a');
        title.href = record.url;
        title.textContent = record.title;
        title.className = 'htmlify-facet-result-title';
        item.appendChild(title);
      }
      if (!thumbnailDisplay) appendMetadata(item, record);
      resultsElement.appendChild(item);
    });
    renderPagination(totalPages);
  }

  function render() {
    renderSelected();
    renderFacets();
    renderResults();
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    state.query = queryInput.value.trim();
    state.page = 1;
    writeState(true);
    render();
  });

  clearButton.addEventListener('click', function () {
    queryInput.value = '';
    state.query = '';
    state.selected = {};
    state.page = 1;
    writeState(true);
    render();
  });

  window.addEventListener('popstate', function () {
    readState();
    render();
  });

  fetch(root.getAttribute('data-index-url')).then(function (response) {
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response.json();
  }).then(function (payload) {
    data = payload;
    prepareData();
    readState();
    render();
  }).catch(function (error) {
    statusElement.textContent = 'The static index could not be loaded.';
    root.classList.add('htmlify-facet-error');
    if (window.console) window.console.error(error);
  });
}());
