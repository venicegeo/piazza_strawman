$(function() {
    $('#geointnav a.search').click(function (e) {
        e.preventDefault();
        $(this).tab('search')
    });

    $('#geointnav a.upload').click(function (e) {
        e.preventDefault();
        $(this).tab('upload')
    });

    var lyr;
    $('#search-form').submit(function(e) { 
        e.preventDefault();
        $.get("api/datasets", $(this).serialize())
         .done(function(data) {
             var table = $("<table class='table'>");
             table.html("<tr><th>Name</th><th>Checksum</th><th>Size</th><th>WMS</th></tr>");
             data.results.forEach(function(row) {
                 var tr = $("<tr>");
                 tr.append($("<td>").text(row.name));
                 tr.append($("<td>").text(row.size));
                 tr.append($("<td>").text(row.native_srid || "<None>"));
                 var td = $("<td>");
                 var btn = $("<button class='btn btn-default'>Create</button>")
                 btn.on('click', function(e) {
                     e.preventDefault();
                     btn.replaceWith($("<a href='#' title='Copy to WMS client'>Capabilities</a>"));
                 });
                 td.append(btn);
                 tr.append(td);
                 table.append(tr);
             });
             $("#search-results").empty().append(table);

             var bboxes = (data.results
                 .filter(function(row) { return row.latlon_bbox != null; })
                 .map(function(row) { return L.rectangle(L.geoJson(row.latlon_bbox).getBounds()); })
             );
             var overall_bounds = bboxes.reduce(function(acc, el) { return acc.extend(el); });
             var newLyr = L.layerGroup(bboxes);
             map.addLayer(newLyr);
             lyr && map.removeLayer(lyr);
             lyr = newLyr;
             map.fitBounds(overall_bounds, { animate: true });
         });
    });

    var map = L.map('map').setView([0, 0], 2);
    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {}).addTo(map);
});

