console.log("Jquery");
$(function() {
    console.log("Ready");
    $('#geointnav a.search').click(function (e) {
        console.log("click");
        e.preventDefault();
        $(this).tab('search')
    });

    $('#geointnav a.upload').click(function (e) {
        console.log("click");
        e.preventDefault();
        $(this).tab('upload')
    });

    $('#search-form').submit(function(e) { 
        e.preventDefault();
        $.get("api/datasets", $(this).serialize())
         .done(function(data) {
             console.log(data);
             var table = $("<table class='table'>");
             table.html("<tr><th>Name</th><th>Checksum</th><th>Size</th></tr>");
             data.results.forEach(function(row) {
                 console.log(row)
                 var tr = $("<tr>");
                 tr.append($("<td>").text(row.name));
                 tr.append($("<td>").text(row.checksum));
                 tr.append($("<td>").text(row.size));
                 table.append(tr);
                 console.log(table);
             });
             $("#search-results").empty().append(table);
         });
    });
});

