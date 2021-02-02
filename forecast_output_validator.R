#' forecast_output_validator
#'
#' @param file_in 
#' @param grouping_variables 
#' @param target_variables 
#'
#' @return
#' @export
#'
#' @examples
forecast_output_validator <- function(file_in, 
                               grouping_variables = c("siteID", "time"),
                               target_variables = c("oxygen", 
                                                    "temperature", 
                                                    "richness",
                                                    "abundance", 
                                                    "nee",
                                                    "le", 
                                                    "vswc",
                                                    "gcc_90")){
  
  lexists <- function(list,name){
    sum(name %in% names(list))
  }
  
  
  valid <- TRUE
  
  if(any(vapply(c("[.]csv", "[.]csv\\.gz"), grepl, logical(1), file_in))){ 
    
    # if file is csv zip file
     out <- readr::read_csv(file_in, guess_max = 1e6)

    if(lexists(out, target_variables) > 0){
      usethis::ui_done("target variables found")
    }else{
      usethis::ui_warn(paste0("no target variables in found in possible list: ", paste(target_variables, collapse = " ")))
      valid <- FALSE
    }
    
    if(lexists(out, "ensemble")){
      usethis::ui_done("file has ensemble members")
    }else if(lexists(out, "statistic")){
      usethis::ui_done("file has summary statistics column")
      if("mean" %in% unique(out$statistic)){
        usethis::ui_done("file has summary statistic: mean")
      }else{
        usethis::ui_warn("files does not have mean in the statistic column")
        valid <- FALSE
      }
      if("sd" %in% unique(out$statistic)){
        usethis::ui_done("file has summary statistic: sd")
      }else{
        usethis::ui_warn("files does not have sd in the statistic column")
        valid <- FALSE
      }
    }else{
      usethis::ui_warn("files does not have ensemble or statistic column")
      valid <- FALSE
    }
    
    if(lexists(out, "siteID")){
      usethis::ui_done("file has siteID column")
    }else{
      usethis::ui_warn("file missing siteID column")
    }
    
    if(lexists(out, "time")){
      usethis::ui_done("file has time column")
      if(sum(class(out$time) %in% c("Date","POSIXct")) > 0){
        usethis::ui_done("file has correct time column")
      }else{
        usethis::ui_warn("time column is incorrect format")
        valid <- FALSE
      }
    }else{
      usethis::ui_warn("file missing time column")
      valid <- FALSE
    }
    
    if(lexists(out, "data_assimilation")){
      usethis::ui_done("file has data_assimilation column")
    }else{
      usethis::ui_warn("file missing data_assimilation column")
      valid <- FALSE
    }
    
    if(lexists(out, "forecast")){
      usethis::ui_done("file has forecast column")
    }else{
      usethis::ui_warn("file missing forecast column")
      valid <- FALSE
    }
  
    
  } else if(grepl("[.]nc", file_in)){ #if file is nc
    
    nc <- ncdf4::nc_open(file_in)
    
    if(lexists(nc$var, target_variables) > 0){
      usethis::ui_done("target variables found")
      var_dim <- dim(ncdf4::ncvar_get(nc, varid = names(nc$var[which(names(nc$var) %in% target_variables)][1])))
    }else{
      usethis::ui_warn(paste0("no target variables in found in possible list: ", paste(target_variables, collapse = " ")))
      valid <- FALSE
    }
    
    if(lexists(nc$dim, "time")){
      usethis::ui_done("file has time dimension")
      time <- ncdf4::ncvar_get(nc, "time")
      time_dim <- length(time)
      tustr<-strsplit(ncdf4::ncatt_get(nc, varid = "time", "units")$value, " ")
      time <-lubridate::as_date(time,origin=unlist(tustr)[3])
      t_string <- strsplit(ncdf4::ncatt_get(nc, varid = "time", "units")$value, " ")[[1]][1]
      if(t_string %in% c("days","seconds")){
        usethis::ui_done("file has correct time dimension")
      }else{
        usethis::ui_warn("time dimension is in correct format")
        valid <- FALSE
      }
    }else{
      usethis::ui_warn("file missing time dimension")
      valid <- FALSE
    }
    
    if(lexists(nc$var, "siteID")){
      usethis::ui_done("file has siteID variable")
    }else{
      usethis::ui_warn("file missing siteID variable")
      valid <- FALSE
    }
    
    if(lexists(nc$dim, c("site")) > 0){
      usethis::ui_done("file has site dimension")
      site_dim <- length(ncdf4::ncvar_get(nc, "site"))

    }else{
      usethis::ui_warn("file missing site dimension")
      valid <- FALSE
    }
    
    if(lexists(nc$dim, "ensemble")){
      usethis::ui_done("file has ensemble dimension")
      ensemble_dim <- length(ncdf4::ncvar_get(nc, "ensemble"))
    }else{
      usethis::ui_warn("file missing ensemble dimension")
      valid <- FALSE
    }
    
    if(var_dim[1] != time_dim){
      usethis::ui_warn("time is not the first dimension")
      valid <- FALSE
    }
    
    if(var_dim[2] != site_dim){
      usethis::ui_warn("site is not the second dimension") 
      valid <- FALSE
    }
    
    if(var_dim[3] != ensemble_dim){
      usethis::ui_warn("ensemble is not the third dimension")
      valid <- FALSE
    }
  }
  
  return(valid)
  
}