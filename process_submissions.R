#remotes::install_deps()

## A place to store everything
fs::dir_create("submissions")
Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "ecoforecast.org")

message("Downloading forecasts ...")

## Note: s3sync stupidly also requires auth credentials even to download from public bucket



#sink(tempfile()) # aws.s3 is crazy chatty and ignores suppressMessages()...
aws.s3::s3sync("submissions", bucket= "submissions",  direction= "download", verbose= FALSE)
#sink()

submissions <- fs::dir_ls("submissions", recurse = TRUE, type = "file")

themes <- c("aquatics", "beetles", "phenology", "terrestrial_30min", "terrestrial_daily", "ticks")
if(length(submissions) > 0){
  for(i in 1:length(submissions)){
    if(length(unlist(stringr::str_split(submissions[i], "/"))) == 3){
      file.copy(submissions[i], file.path("submissions", basename(submissions[i])))
      submissions[i] <- file.path("submissions", basename(submissions[i]))
    }
    curr_submission <- basename(submissions[i])
    theme <-  stringr::str_split(curr_submission, "-")[[1]][1]
    submission_date <- lubridate::as_date(paste(stringr::str_split(curr_submission, "-")[[1]][2:4], 
                                                collapse = "-"))
    print(i)
    print(curr_submission)
    print(theme)
    
    if((tools::file_ext(curr_submission) %in% c("nc", "gz", "csv", "xml")) & !is.na(submission_date)){
      
      log_file <- paste0("submissions/",curr_submission,".log")
      
      if(theme %in% themes & submission_date <= Sys.Date()){
        
        capture.output({
          valid <- tryCatch(neon4cast::forecast_output_validator(file.path("submissions",curr_submission)),
                            error = function(e) FALSE, 
                            finally = NULL)
        }, file = log_file, type = c("message"))
        
        if(valid){
          aws.s3::copy_object(from_object = curr_submission, 
                              to_object = paste0(theme,"/",curr_submission), 
                              from_bucket = "submissions", 
                              to_bucket = "forecasts")
          if(aws.s3::object_exists(object = paste0(theme,"/",curr_submission), bucket = "forecasts")){
            print("delete")
            aws.s3::delete_object(object = curr_submission, bucket = "submissions")
          }
        }else{
          aws.s3::copy_object(from_object = curr_submission, 
                              to_object = paste0("not_in_standard/",curr_submission), 
                              from_bucket = "submissions", 
                              to_bucket = "forecasts")
          if(aws.s3::object_exists(object = paste0("not_in_standard/",curr_submission), bucket = "forecasts")){
            print("delete")
            aws.s3::delete_object(object = curr_submission, bucket = "submissions")
          }
          
          aws.s3::put_object(file = log_file, 
                             object = paste0("not_in_standard/", 
                                             basename(log_file)), 
                             bucket = "forecasts")
        }
      }else if(!(theme %in% themes)){
        aws.s3::copy_object(from_object = curr_submission, 
                            to_object = paste0("not_in_standard/",curr_submission), 
                            from_bucket = "submissions",
                            to_bucket = "forecasts")
        capture.output({
          message(curr_submission)
          message("incorrect theme name in filename")
          message("Options are: ", paste(themes, collapse = " "))
        }, file = log_file, type = c("message"))
        
        if(aws.s3::object_exists(object = paste0("not_in_standard/",curr_submission), bucket = "forecasts")){
          print("delete")
          aws.s3::delete_object(object = curr_submission,
                                bucket = "submissions")
        }
        
        aws.s3::put_object(file = log_file,
                           object = paste0("not_in_standard/", 
                                           basename(log_file)), 
                           bucket = "forecasts")
      }else{
        #Don't do anything because the date hasn't occur yet
      }
    }else{
      aws.s3::copy_object(from_object = curr_submission, 
                          to_object = paste0("not_in_standard/",curr_submission), 
                          from_bucket = "submissions",
                          to_bucket = "forecasts")
    }
  }
}
unlink("submissions",recursive = TRUE)
