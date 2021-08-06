#remotes::install_deps()

Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "ecoforecast.org")

submissions_directory <- "/efi_neon_challenge/submissions"

#source("forecast_output_validator.R")

object <- aws.s3::get_bucket("submissions", max = Inf)

themes <- c("aquatics", "beetles", "phenology", "terrestrial", "ticks")
if(length(object) > 0){
  for(i in 1:length(object)){
    theme <-  stringr::str_split(object[[i]]$Key, "-")[[1]][1]
    theme <- stringr::str_split(theme, "_")[[1]][1]
    submission_date <- lubridate::as_date(paste(stringr::str_split(object[[i]]$Key, "-")[[1]][2:4], collapse = "-"))
    print(i)
    print(object[[i]]$Key)
    print(theme)
    subdirectory_present <- stringr::str_detect(object[[i]]$Key, "/")
    
    
    if(!stringr::str_detect(object[[i]]$Key, "processed") & (tools::file_ext(object[[i]]$Key) %in% c("nc", "gz", "csv", "xml")) & !is.na(submission_date) & !subdirectory_present){
      
      log_file <- paste0(submissions_directory,"/",object[[i]]$Key,".log")
      
      if(theme %in% themes & submission_date <= Sys.Date()){
        
        capture.output({
          valid <- tryCatch(neon4cast::forecast_output_validator(file.path(submissions_directory,object[[i]]$Key)),error = function(e) FALSE, finally = NULL)
        }, file = log_file, type = c("message"))
        
        if(valid){
          aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0(theme,"/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
          aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("processed/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "submissions")
          if(aws.s3::object_exists(object = paste0(theme,"/",object[[i]]$Key), bucket = "forecasts")){
            print("delete")
            aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
          }
            aws.s3::delete_object(object = basename(log_file), bucket = "submissions")
            
        }else{
          aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("not_in_standard/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
          aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("processed/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "submissions")
          aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
          if(aws.s3::object_exists(object = paste0("not_in_standard/",object[[i]]$Key), bucket = "forecasts")){
            print("delete")
            aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
          }
          
          aws.s3::put_object(file = log_file, object = paste0("not_in_standard/", basename(log_file)), bucket = "forecasts")
          if(aws.s3::object_exists(object = paste0("not_in_standard/", basename(log_file)), bucket = "forecasts")){
            aws.s3::delete_object(object = basename(log_file), bucket = "submissions")
          }
        }
      }else if(!(theme %in% themes)){
        aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("not_in_standard/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
        aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0("processed/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "submissions")
        capture.output({
          message(object[[i]]$Key)
          message("incorrect theme name in filename")
          message("Options are: ", paste(themes, collapse = " "))
        }, file = log_file, type = c("message"))
        
        if(aws.s3::object_exists(object = paste0("not_in_standard/",object[[i]]$Key), bucket = "forecasts")){
          print("delete")
          aws.s3::delete_object(object = object[[i]]$Key, bucket = "submissions")
        }
        
        aws.s3::put_object(file = log_file, object = paste0("not_in_standard/", basename(log_file)), bucket = "forecasts")
        if(aws.s3::object_exists(object = paste0("not_in_standard/", basename(log_file)), bucket = "forecasts")){
          aws.s3::delete_object(object = basename(log_file), bucket = "submissions")
        }
      }else{
        #Don't do anything
      }
    }
  }
}
