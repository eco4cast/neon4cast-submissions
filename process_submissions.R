remotes::install_deps()

Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "ecoforecast.org")


object <- aws.s3::get_bucket("submissions")

themes <- c("aquatics", "beetles", "phenology", "terrestrial", "ticks")
if(length(object) > 0){
  for(i in 1:length(object)){
    theme <-  stringr::str_split(object[[i]]$Key, "-")[[1]][1]
    theme <- stringr::str_split(theme, "_")[[1]][1]
    print(object[[i]]$Key)
    print(theme)
    if(theme %in% themes){
      aws.s3::copy_object(from_object = object[[i]]$Key, to_object = paste0(theme,"/",object[[i]]$Key), from_bucket = "submissions", to_bucket = "forecasts")
    }
  }
}
