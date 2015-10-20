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
             table.html("<tr><th>Name</th><th>Checksum</th><th>Size</th></tr>");
             data.results.forEach(function(row) {
                 var tr = $("<tr>");
                 tr.append($("<td>").text(row.name));
                 tr.append($("<td>").text(row.checksum));
                 tr.append($("<td>").text(row.size));
                 table.append(tr);
             });
             $("#search-results").empty().append(table);
         });
    });
});

