"use strict";


var GRAPH = GRAPH || {};  // namespace


/*
 ******************************************************************************
 ******************************************************************************
 */
GRAPH.GraphManager = function(args) {
    this.data = args.data;
    this.metrics = args.metrics;
    this.seriesly = args.seriesly;
};


GRAPH.GraphManager.prototype.init = function() {
    var dataHandler = new GRAPH.DataHandler(this.data);
    var series_data = dataHandler.prepareSeries(this.metrics);

    var format = d3.time.format("%H:%M:%S");
    nv.addGraph(function() {
        var chart = nv.models.lineWithFocusChart();

        chart.xAxis
            .tickFormat(format);
        chart.x2Axis
            .tickFormat(format);
        chart.yAxis
            .tickFormat(d3.format(',.2f'));
        chart.y2Axis
            .tickFormat(d3.format(',.2f'));

        d3.select('#chart svg')
            .datum(series_data)
            .call(chart);

        return chart;
    });
};



/*
 ******************************************************************************
 ******************************************************************************
 */

GRAPH.DataHandler = function(data) {
    this.data = data;
    this.timestamps = this.prepareTimestamps();
};


GRAPH.DataHandler.prototype.prepareTimestamps = function() {
    var timestamps = [];
    for(var timestamp in this.data) {
        if (this.data.hasOwnProperty(timestamp)) {
            timestamps.push(parseInt(timestamp, 10));
        }
    }
    return timestamps.sort();
};


GRAPH.DataHandler.prototype.prepareSeries = function(metrics) {
    var i, j,
        len, len_metrics,
        timestamp,
        series = [];
    for(i = 0, len = metrics.length; i < len; i++) {
        series.push({
            key: metrics[i],
            values: []
        });
    }

    for(i = 0, len = this.timestamps.sort().length; i < len; i++) {
        timestamp = this.timestamps[i];
        for(j = 0, len_metrics = metrics.length; j < len_metrics; j++) {
            series[j].values.push({
                x: timestamp,
                y: this.data[timestamp][j] / 1024 / 1024
            })
        }
    }
    return series;
};
