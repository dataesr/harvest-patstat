$('#hitchhiker').on('click', function() {
    $.ajax({
        url: '/query',
        contentType: 'application/json; charset=utf-8',
        dataType: 'json',
        method: 'GET'
    })
    .done(res => {
        $('#res_query').html(res.res);
    })
    .fail(err => {
        console.log(err)
    });
});

$('#patstat').html('Hello World bis!');

