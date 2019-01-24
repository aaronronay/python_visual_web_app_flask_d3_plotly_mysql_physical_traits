// --------------------------------------------------------------------------
// Initializes names function tied to Flask app - populates selector
// --------------------------------------------------------------------------
d3.json("/names", function(error, response) {
    // error trapping for failed call from Flask
    if (error) return console.warn(error);

    var $dropDown = document.getElementById("selDataset");

    for (var i = 0; i < response.length; i++) {
        var $optionChoice = document.createElement("option");
        $optionChoice.innerHTML = response[i];
        $optionChoice.setAttribute("value", response[i]);
        $dropDown.appendChild($optionChoice);
    };
});

// --------------------------------------------------------------------------
// Initializes webpage render from template
// --------------------------------------------------------------------------
function init(sample) {
    getMetadata(sample);
    // ----------------------------------------------------------------------
    // Defines pie chart functionality
    // ----------------------------------------------------------------------
    d3.json("/samples/" + sample, function(error, sampleResponse) {
        // error trapping for failed call from Flask
        if (error) return console.warn(error);

        // parses response and takes top-ten slice
        resLabels = sampleResponse[0]["otu_ids"].slice(0,10);
        resValues = sampleResponse[1]["sample_values"].slice(0,10);

        for (var i = 0; i < 10; i++) {
            if (resLabels[i] == 0) {
                resLabels = resLabels.slice(0, i)
            };
            if (resValues[i] == 0) {
                resValues[i] = resValues.slice(0, i)
            };
        };

        // ------------------------------------------------------------------
        // get description matches of top ten bacteria and creates list
        // ------------------------------------------------------------------
        d3.json("/otu_descriptions", function(error, otuResponse) {
        // error trapping for failed call from Flask
        if (error) return console.warn(error);

        var bacteriaNamesPie = [];
        for (var i = 0; i < resLabels.length; i++) {
            bacteriaNamesPie.push(otuResponse[resLabels[i]])
        };

        //  list of names for Bubble Chart
        var bacteriaNamesBub = [];
        for (var i = 0; i < sampleResponse[0]["otu_ids"].length; i++) {
            bacteriaNamesBub.push(otuResponse[sampleResponse[0]["otu_ids"][i]])
        };

        // sets up data for pie chart
        var data = [{values: resValues,
                     labels: resLabels,
                     hovertext: bacteriaNamesPie,
                     hoverinfo: {bordercolor: 'black'},
                     type: 'pie'}];

        // sets up layout for pie chart
        var layout = {height: 500,
                      title: "Top Sample Counts for " + sample};

        // plots piechart
        Plotly.newPlot('piePlot', data, layout);

        // ------------------------------------------------------------------
        // Defines bubble chart functionality
        // ------------------------------------------------------------------
        var trace = {x: sampleResponse[0]["otu_ids"],
                     y: sampleResponse[1]["sample_values"],
                     mode: 'markers',
                     marker: {colorscale: 'Earth',
                              color: sampleResponse[0]["otu_ids"],
                              size: sampleResponse[1]["sample_values"]},
                     text: bacteriaNamesBub,
                     type: "scatter"};

        var bubbleData = [trace];

        var bubbleLayout = {title: 'Sample Values for ' + sample,
                            xaxis: {title: "OTU ID"},
                            hovermode: 'closest',
                            showlegend: false,
                            height: 600,
                            margin: {top: 10,
                                     bottom: 10,
                                     right: 10,
                                     left: 10}};

        // plots bubble chart
        Plotly.newPlot('bubblePlot', bubbleData, bubbleLayout);
      });
    });

    // ----------------------------------------------------------------------
    // Defines meter chart function
    // ----------------------------------------------------------------------
    d3.json("/wfreq/" + sample, function(error, washResponse){
        // error trapping for failed call from Flask
        if (error) return console.warn(error);

        var path = getPointerCoords(washResponse);

        var data = [{type: 'scatter',
                     x: [0],
                     y:[0],
                     marker: {size: 15, color:'850000'},
                     showlegend: false,
                     name: 'Washes',
                     hoverinfo: 'name'},
        { values: [50 / 9, 50 / 9, 50 / 9, 50 / 9, 50 / 9, 50 / 9, 50 / 9, 50 / 9, 50 / 9, 50],
          rotation: 90,
          text: ["8-9", "7-8", "6-7", "5-6", "4-5", "3-4", "2-3", "1-2", "0-1", ""],
          textinfo: 'text',
          textposition:'inside',
          marker: {colors: ['rgba(0, 255, 0, .5)',
                            'rgba(60, 255, 60, .5)',
                            'rgba(150, 255, 150, .5)',
                            'rgba(178, 255, 102, .5)',
                            'rgba(255, 255, 102, .5)',
                            'rgba(255, 178, 102, .5)',
                            'rgba(255, 150, 150, .5)',
                            'rgba(255, 60, 60, .5)',
                            'rgba(255, 0, 0, .5)',
		                        'rgba(255, 255, 255, 0)']},
          labels: ["8-9", "7-8", "6-7", "5-6", "4-5", "3-4", "2-3", "1-2", "0-1"],
          hoverinfo: 'label',
          hole: 0.5,
          type: 'pie',
          showlegend: false}];

        var layout = {shapes:[{type: 'path',
                               path: path,
                               fillcolor: '850000',
                               line: {color: '850000'}}],
            title: "<b>Belly Button Washing Frequency</b> <br> Washes per Week",
            height: 500,
            xaxis: {zeroline:false, showticklabels:false,
                    showgrid: false, range: [-1, 1]},
            yaxis: {zeroline:false, showticklabels:false,
                    showgrid: false, range: [-1, 1]}};
        // plots wash frequency meter
        Plotly.newPlot('meter', data, layout);
    });
    console.log("Initial Render Successful!");};

// update pie chart function
function updatePie(newValues, newLabels, newNames, sample_name){
    Plotly.restyle("piePlot", "values", [newValues]);
    Plotly.restyle("piePlot", "labels", [newLabels]);
    Plotly.restyle("piePlot", "hovertext", [newNames]);
    Plotly.relayout("piePlot", "title", "Top Sample Counts for " + sample_name);
    console.log("Pie Chart Updated!");};

// update bubble chart function
function updateBub(values, labels, names, sample_name){
    Plotly.restyle("bubblePlot", "x", [labels]);
    Plotly.restyle("bubblePlot", "y", [values]);
    Plotly.restyle("bubblePlot", "marker.size", [values]);
    Plotly.restyle("bubblePlot", "text", [names]);
    Plotly.relayout("bubblePlot", "title", "Sample Values for " + sample_name);
    console.log("Bubble Chart Updated!");};

// update gauge chart function
function updateMeter(newWashFreq) {
    var path = getPointerCoords(newWashFreq);
    Plotly.relayout("meter", "shapes[0].path", path);
    console.log("Wash Frequency Meter Updated!");};

// handles change in dropdown
function optionChanged(chosenSample) {
  getMetadata(chosenSample);

  // handle new get request for choice
  d3.json("/samples/" + chosenSample, function(error, newResponse) {
  // error trapping for failed call from Flask
  if (error) return console.warn(error);

  var newResLabels = newResponse[0]["otu_ids"].slice(0,10);
  var newResValues = newResponse[1]["sample_values"].slice(0,10);

  for (var i = 0; i < 10; i++){
    if (newResLabels[i] == 0){
        newResLabels = resLabels.slice(0,i)
    };
    if (newResValues[i] == 0){
        newResValues[i] = resValues.slice(0,i)
    };
  };

  d3.json("/otu_descriptions", function(error, newOtuResponse) {
  // error trapping for failed call from Flask
  if (error) return console.warn(error);

  var newBacteriaNames = [];

  for (var i = 0; i < newResLabels.length; i++) {
      newBacteriaNames.push(newOtuResponse[newResLabels[i]])};
  //  all bacteria names for bubble hover
  var allBacteriaNames = [];
  for (var i = 0; i < newResponse[0]["otu_ids"].length; i++) {
      allBacteriaNames.push(newOtuResponse[newResponse[0]["otu_ids"][i]])};

  // new variables for updateBub function
  var newValuesBub = newResponse[1]['sample_values'];
  var newLabelsBub = newResponse[0]['otu_ids'];

  // update plots
  updatePie(newResValues, newResLabels, newBacteriaNames, chosenSample);
  updateBub(newValuesBub, newLabelsBub, allBacteriaNames, chosenSample);
  })

  d3.json("/wfreq/" + chosenSample, function(error, washResponse) {
  // error trapping for failed call from Flask
  if (error) return console.warn(error);
      updateMeter(washResponse);
  });
  });
};

function getPointerCoords(targetSample) {
  var level = targetSample * 20;

  // calculates meter pointer location using trigonometric functions
  var degrees = 180 - level;
  var radius = .5;
  var radians = degrees * Math.PI / 180;
  var x = radius * Math.cos(radians);
  var y = radius * Math.sin(radians);

  var mainPath = 'M -.0 -0.025 L .0 0.025 L ';
  var space = ' ';
  var pathEnd = 'Z';
  var pathX = x.toFixed(2);
  var pathY = y.toFixed(2);
  var path = mainPath.concat(pathX, space, pathY, space, pathEnd);

  return path;
};

function getMetadata(selectedID) {
  d3.json("/metadata/" + selectedID, function(error, response) {
    // error trapping for failed call from Flask
    if (error) return console.warn(error);

    // get list of keys from response
    var resKeys = Object.keys(response);

    // tie to div in template
    var $sampleMetadata = document.querySelector("#sample-metadata");

    // clears panel
    $sampleMetadata.innerHTML = null;

    // iterates through keys and creates p element for each key
    for (var i = 0; i < resKeys.length; i++) {
        var $newDataLine = document.createElement('p');
        $newDataLine.innerHTML = resKeys[i] + ": " + response[resKeys[i]];
        $sampleMetadata.appendChild($newDataLine)
    };
  });
};

// initial render using first sample in dataset
init("BB_940");
