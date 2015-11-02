$(function() {
    $('#piazzanav a.search').click(function (e) {
        e.preventDefault();
        $(this).tab('search')
    });

    $('#piazzanav a.upload').click(function (e) {
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
                 tr.append($("<td>").text(row.checksum));
                 tr.append($("<td>").text(row.size));
                 var td = $("<td>");
                 if (row.deploymentServer != null) {
                     td.append($("<a href='/api/deployments?dataset=" + row.locator + "&SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities' title='Copy to WMS client'>Capabilities</a>"))
                 } else if (row.nativeSrid != null) {
                     var btn = $("<button class='btn btn-default'>Create</button>");
                     td.append(btn);
                     btn.on('click', function() {
                         $.ajax({
                             url: "/api/deployments/", 
                             method: "POST",
                             data: { "dataset": row.locator },
                             success: function(data) { 
                                 var server = data.servers[0];
                                 btn.replaceWith($("<a href='/api/deployments?dataset=" + row.locator + "&SERVICE=WMS&VERSION=1.3.0&REQUEST=GetCapabilities' title='Copy to WMS client'>Capabilities</a>"));
                             }
                         });
                     });
                 } else {
                     td.text("Not spatial");
                 }
                 tr.append(td);
                 table.append(tr);
             });
             $("#search-results").empty().append(table);

             var bboxes = (data.results
                 .filter(function(row) { return row.latlonBbox != null; })
                 .map(function(row) { return L.rectangle(L.geoJson(row.latlonBbox).getBounds()); })
             );
             var overallBounds = bboxes.reduce(function(acc, el) { return acc.extend(el); });
             var newLyr = L.layerGroup(bboxes);
             map.addLayer(newLyr);
             lyr && map.removeLayer(lyr);
             lyr = newLyr;
             map.fitBounds(overallBounds, { animate: true });
         });
    });

    var map = L.map('map').setView([0, 0], 2);
    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {}).addTo(map);
});
