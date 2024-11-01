library(tidyverse)
library(ggplot2)
library(ggimage)
library(plotly)

team1 <- "Leicester"
team2 <- "Arsenal"

Animals <- c("Match")
Win_Home <- 0.4
Draw <- 0.1
Win_Away <- 0.5
data <- data.frame(Animals, Win_Home, Draw, Win_Away)

marker_style <- list(line = list(width = 5,
                                 color = 'rgb(255, 255, 255)'))


p <- plot_ly(data, type = 'bar', width = 1200, height = 200) %>%
  add_trace(y = ~Animals, x = ~Win_Home,  width = 0.4,
            hovertext = paste0("Predicted probability that ",team1," wins:",round(Win_Home[1]*100)," %"),
            orientation = "h", hoverinfo='text',
            marker = list(color = 'grey', line = list(width = 5, color = 'rgb(255, 255, 255)'))) %>%
  add_trace(y = ~Animals, x = ~Draw,  width = 0.4, 
            hovertext = paste0("Predicted probability of a draw: ",round(Draw[2]*100),"%"),
            orientation = "h",hoverinfo='text',
            marker = list(color = 'grey', line = list(width = 5, color = 'rgb(255, 255, 255)'))) %>%
  add_trace(y = ~Animals, x = ~Win_Away, width = 0.4, 
            hovertext = paste0("Predicted probability that ",team2," wins: ",round(Win_Home[3]*100),"%"),
            orientation = "h",hoverinfo='text',
            marker = list(color = 'red', line = list(width = 5, color = 'rgb(255, 255, 255)'))) %>%
  layout(barmode = 'stack', 
         title = "",
         showlegend = FALSE,
         autosize = FALSE,
         paper_bgcolor="white",
         
         margin = list(l=250, r=250, t=10, b=10),
         xaxis = list(title = "", showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE, range = list(0,1)),
         yaxis = list(title = "", showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE, range = list(-0.5,0.5)),
         images = list(  
           list(  
             source =  "https://cdn.ssref.net/req/202410231/tlogo/fb/18bb7c10.png",  
             xref = "paper",  
             yref = "paper",  
             x = 1.25,  
             y = 0.55,  
             sizex = 0.5,  
             sizey = 0.5,  
             xanchor="center",  
             yanchor="middle" 
           ),
           list(  
             source =  "https://cdn.ssref.net/req/202410231/tlogo/fb/a2d435b3.png",  
             xref = "paper",  
             yref = "paper",  
             x = -0.25,  
             y = 0.55,  
             sizex = 0.5,  
             sizey = 0.5,  
             xanchor="center",  
             yanchor="middle" 
           )
         ) ) %>% 
  add_annotations(x = 0.4/2,
                  y = 0.8,
                  text = "40%",
                  xref = "x",
                  yref = "paper",
                  showarrow = FALSE) %>% 
  add_annotations(x = 0.4+0.1/2,
                  y = 0.8,
                  text = "10%",
                  xref = "x",
                  yref = "paper",
                  showarrow = FALSE) %>% 
  add_annotations(x = 0.4+0.1+0.5/2,
                  y = 0.8,
                  text = "50%",
                  xref = "x",
                  yref = "paper",
                  showarrow = FALSE) %>% 
  add_annotations(text="01 December 2022",
                 xref="x", yref="paper", xanchor = "center",yanchor="middle" ,
                 x=0.5, y=0, showarrow=FALSE) %>% 
  add_annotations(text="Leicester City",
                  xref="paper", yref="paper", xanchor = "center",yanchor="middle" ,
                  x=-0.25, y=0, showarrow=FALSE) %>% 
  add_annotations(text="Arsenal London",
                  xref="paper", yref="paper", xanchor = "center",yanchor="middle" ,
                  x=1.25, y=0, showarrow=FALSE)
p
