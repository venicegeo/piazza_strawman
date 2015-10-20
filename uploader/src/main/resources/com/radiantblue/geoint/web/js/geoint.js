$(function() {
    $('#geointnav a.search').click(function (e) {
        e.preventDefault();
        $(this).tab('search')
    });

    $('#geointnav a.upload').click(function (e) {
        e.preventDefault();
        $(this).tab('upload')
    });

    $('#search-form').submit(function(e) { 
        e.preventDefault();
        $.get("api/datasets", $(this).serialize())
         .done(function(data) {
             var table = $("<table class='table'>");
             table.html("<tr><th>Name</th><th>Checksum</th><th>Size</th><th>SRID</th></tr>");
             data.results.forEach(function(row) {
                 var tr = $("<tr>");
                 tr.append($("<td>").text(row.name));
                 tr.append($("<td>").text(row.checksum));
                 tr.append($("<td>").text(row.size));
                 tr.append($("<td>").text(row.native_srid || "<None>"));
                 table.append(tr);
             });
             $("#search-results").empty().append(table);

             var bboxes = (data.results
                 .filter(function(row) { return row.latlon_bbox != null; })
                 .map(function(row) { return L.rectangle(L.geoJson(row.latlon_bbox).getBounds()); })
             );
             var overall_bounds = bboxes.reduce(function(acc, el) { return acc.extend(el); });
             var lyr = L.layerGroup(bboxes);
             lyr.addTo(map); // L.geoJson(bboxes, { style: function() { return { "color" : "red" }; } }).addTo(map);
             map.fitBounds(overall_bounds);
         });
    });

    var map = L.map('map').setView([51.05, -0.09], 13);
    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {}).addTo(map);
});

