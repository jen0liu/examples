library(shiny)
library(jsonlite)

# Define UI for application that draws a histogram
shinyUI(fluidPage(
				
				# Application title
				titlePanel("Cluster functions"),
				
				tags$style(type="text/css",
						".shiny-output-error { visibility: hidden; }",
						".shiny-output-error:before { visibility: hidden; }"
				),

				tags$head(tags$style(type="text/css", "#someid {width: 1000px}")),
				
				sidebarLayout(
						sidebarPanel(
								radioButtons("selectionPath","Selecting Entry Path:",
										choices = c("Cluster Direct" = "byCluster",
												"Template Configs" = "byTemplate"),
										selected = "byCluster"),
								uiOutput("selectionConfigId"),      
								uiOutput("ids"),
								uiOutput("demos"),
								uiOutput("favs"),
								uiOutput("topics"),
								uiOutput("level3"),
								uiOutput("tags"),
								
								uiOutput("functions"),
								conditionalPanel(
										condition="input.functions=='timeseries'",
										checkboxInput("scale", "Scale by Sample", value = TRUE),
										checkboxInput("correlate", "Correlate with Revenue", value = TRUE),
										radioButtons("groupBy","Group By Period:",
												choices = c("Day" = "byDay",
														"Week" = "byWeek", 
														"Month" = "byMonth", 
														"Year" = "byYear"),
												selected = "byWeek"
										)										
								),
								uiOutput("dateRange")
								,actionButton("goButton", "Run"),
								p("When changing any selection here, click run to update results.")
							
						),
						
						mainPanel(      
#								h4("Time Series:"),
								verbatimTextOutput("docs"),
								downloadButton('downloadData', 'Download Raw Data'),
								tableOutput('rawData'),
								plotOutput("distPlot"),
								verbatimTextOutput("compares"),
								downloadButton('compareData', 'Download Compare Data'),
								tableOutput('compareCounts'),								
								plotOutput("comparePlot"),
								plotOutput("comparePlotScaled")

						)
				)
		))
