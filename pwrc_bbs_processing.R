# joined tables bbsfifty

# working on bbsfifty_1

##--                            --##

##--country|species|state tables--##

##--                            --##

species_list <- read.csv("~/R/pwrc-bbs/species_list.csv")

country_codes <- read.csv("~/R/pwrc-bbs/country_codes.csv")

state_codes <- read.csv("~/R/pwrc-bbs/state_codes.csv")

routes <- read.csv("~/R/pwrc-bbs/routes.csv")



# format aou code in species_list table

species_list$aou<-sprintf("%05d",species_list$aou)

# format country code in country_codes table

country_codes$country_code<-sprintf("%03d",country_codes$country_code)

# format country code in state_codes table

state_codes$countrynum<-sprintf("%03d",state_codes$countrynum)

# format state code in state_codes table

state_codes$regioncode<-sprintf("%02d",state_codes$regioncode)

# format country code in routes table

routes$countrynum<-sprintf("%03d",routes$countrynum)

# format state code in routes table

routes$statenum<-sprintf("%02d",routes$statenum)

# format route code in routes table

routes$Route<-sprintf("%03d",routes$Route)

# concatenate country, state in state_codes table to create lookup field for state

state_codes$cntrystatenum <- paste(state_codes$countrynum, state_codes$regioncode, sep='')

# concatenate country and state code in Route table to create lookup field for country/state

routes$cntrystatenum <- paste(routes$countrynum, routes$statenum, sep='')

# concatenate country, state, route code in Route table to create lookup field for country/state/route lat/lon

routes$cntrystateroutenum <- paste(routes$cntrystatenum, routes$Route, sep='')


##--                            --##

##--bbs table format and process--##

##--                            --##

# import bbs data

bbsfifty <- read.csv("~/R/pwrc-bbs/FiftyStopData/fifty10.csv")

# add observations from all stops

bbsfifty$stopstotalcount <- rowSums(bbsfifty[, c(8:57)])

# concatenate into counts for each stop

bbsfifty$stopcount <- do.call(paste, c(bbsfifty[8:57], sep=";"))

# clean up

# remove unwanted columns

bbsfifty<-bbsfifty[,-c(8:57)]

# format country code in main table

bbsfifty$countrynum<-sprintf("%03d",bbsfifty$countrynum)

# format state code in main table

bbsfifty$statenum<-sprintf("%02d",bbsfifty$statenum)

# format route code in main table

bbsfifty$Route<-sprintf("%03d",bbsfifty$Route)

# concatenate country and state code in main table to create lookup field for country/state

bbsfifty$cntrystatenum <- paste(bbsfifty$countrynum, bbsfifty$statenum, sep='')

# concatenate country, state, route code in main table to create lookup field for country/state/route lat/lon

bbsfifty$cntrystateroutenum <- paste(bbsfifty$cntrystatenum, bbsfifty$Route, sep='')

# Join main table and country lookup to retrieve country

countryjoinstr <- "select bbsfifty.*, country_codes.country from bbsfifty left join country_codes on bbsfifty.countrynum = country_codes.country_code"

bbsfifty_join_temp <- sqldf(countryjoinstr)

# join main table and state_codes to retrieve state

statejoinstr <- "select bbsfifty_join_temp.*, state_codes.state from bbsfifty_join_temp left join state_codes on bbsfifty_join_temp.cntrystatenum = state_codes.cntrystatenum"

bbsfifty_join_temp <- sqldf(statejoinstr)

# format aou code in main table

bbsfifty_join_temp$AOU<-sprintf("%05d",bbsfifty_join_temp$AOU)

# lookup species scientific name

speciesaoujoinstr <- "select bbsfifty_join_temp.*,species_list.english_common_name,species_list.species_name from bbsfifty_join_temp left join species_list on bbsfifty_join_temp.AOU=species_list.aou"

bbsfifty_join_temp <- sqldf(speciesaoujoinstr)

# lookup lat/lon

latlonjoinstr <- "select bbsfifty_join_temp.*, routes.Lati, routes.Longi, routes.Active from bbsfifty_join_temp left join routes on bbsfifty_join_temp.cntrystateroutenum = routes.cntrystateroutenum"

bbsfifty_join_temp <- sqldf(latlonjoinstr)

# clean up


# rename columns

colnames(bbsfifty_join_temp)[1]<-"routedataid"

colnames(bbsfifty_join_temp)[4]<-"route"

colnames(bbsfifty_join_temp)[5]<-"rpid"

colnames(bbsfifty_join_temp)[7]<-"aou"

colnames(bbsfifty_join_temp)[12]<-"iso_country"

colnames(bbsfifty_join_temp)[13]<-"provided_state_name"

colnames(bbsfifty_join_temp)[14]<-"provided_common_name"

colnames(bbsfifty_join_temp)[15]<-"provided_scientific_name"

colnames(bbsfifty_join_temp)[16]<-"latitude"

colnames(bbsfifty_join_temp)[17]<-"longitude"

colnames(bbsfifty_join_temp)[18]<-"active"


# write to file append

write.table(bbsfifty_join_temp, file = "bbs_preprocessed_10.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)

write.table(bbsfifty_join_temp, file = "bbs_preprocessed_6-10.txt", append = TRUE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)


# trim 

trim <- function (x) gsub("^\\s+|\\s+$", "", x)

bbsfifty_join$species_name<-trim(bbsfifty_join$species_name)

# write out

write.csv(bbsfifty_join,"bbsfifty_partial_processed_2015-02-09.csv",row.names=FALSE)







