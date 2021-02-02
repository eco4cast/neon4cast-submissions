remotes::install_deps()

Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "ecoforecast.org")

submissions_directory <- "/efi_neon_challenge/submissions"

source("forecast_output_validator.R")


object <- aws.s3::get_bucket("submissions")

themes <- c("aquatics", "beetles", "phenology", "terrestrial", "ticks")
if(length(object) > 0){
  for(i in 1:length(object)){
    theme <-  stringr::str_split(object[[i]]$Key, "-")[[1]][1]
    theme <- stringr::str_split(theme, "_")[[1]][1]
    print(object[[i]]$Key)
    print(theme)
    
    log_file <- paste0(submissions_directory,"/",object[[i]]$Key,".log")
    
    if(theme %in% themes & tools::file_ext(object[[i]]$Key) != "log"){
      
      capture.output({
        valid <- tryCatch(forecast_output_validator(file.path(submissions_directory,object[[i]]$Key)),error = function(e) FALSE, finally = NULL)
      }, file = log_file, type = c("message"))
      
      if(valid){
        aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0(theme,"/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
        success_tranfer <- tryCatch(aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("processed/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "submissions"), error = function(e) NA_real_,finally = NULL)
        aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
      }else{
        aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("forecasts/not_in_standard/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
        success_tranfer <- tryCatch(aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("processed/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "submissions"), error = function(e) NA_real_,finally = NULL)
        aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
        aws.s3::put_object(file = log_file, bucket = "forecasts/not_in_standard")
      }
    }else{
      aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("forecasts/not_in_standard/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
      success_tranfer <- tryCatch(aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("processed/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "submissions"), error = function(e) NA_real_,finally = NULL)
      capture.output({
      message(object[[i]]$Key)
      message("incorrect theme name in filename")
      message("Options are: ", paste(themes, collapse = " "))
      }, file = log_file, type = c("message"))
      aws.s3::put_object(file = log_file, bucket = "forecasts/not_in_standard")
      
    }
    
    if(!is.na(success_tranfer)){
      aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
    }
    unlink(log_file)
  }
}