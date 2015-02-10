# joined tables bbsfifty

# working on bbsfifty_2

# import data

# bbs observations

# routes

# species list

# state codes

# country codes

# add observations from all stops

bbsfifty$stopstotalcount <- rowSums(bbsfifty[, c(8:57)])

# concatenate into counts for each stop

bbsfifty_temp<-bbsfifty

bbsfifty$stopcount <- do.call(paste, c(bbsfifty[8:57], sep=";"))

# clean up

# remove unwanted columns

bbsfifty<-bbsfifty[,-c(8:57)]

# format country code in country_codes table

country_codes$country_code<-sprintf("%03d",country_codes$country_code)


# format country code in main table

bbsfifty$countrynum<-sprintf("%03d",bbsfifty$countrynum)

# format state code in main table

bbsfifty$statenum<-sprintf("%02d",bbsfifty$statenum)

# format route code in main table

bbsfifty$Route<-sprintf("%03d",bbsfifty$Route)

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

# concatenate country and state code in main table to create lookup field for country/state

bbsfifty$cntrystatenum <- paste(bbsfifty$countrynum, bbsfifty$statenum, sep='')

# concatenate country, state, route code in main table to create lookup field for country/state/route lat/lon

bbsfifty$cntrystateroutenum <- paste(bbsfifty$cntrystatenum, bbsfifty$Route, sep='')

# concatenate country, state in state_codes table to create lookup field for state

state_codes$cntrystatenum <- paste(state_codes$countrynum, state_codes$regioncode, sep='')

# concatenate country and state code in Route table to create lookup field for country/state

routes$cntrystatenum <- paste(routes$countrynum, routes$statenum, sep='')

# concatenate country, state, route code in Route table to create lookup field for country/state/route lat/lon

routes$cntrystateroutenum <- paste(routes$cntrystatenum, routes$Route, sep='')

# format aou code in main table

bbsfifty$AOU<-sprintf("%05d",bbsfifty$AOU)

# format aou code in species_list table

species_list$aou<-sprintf("%05d",species_list$aou)

# lookup species scientific name

speciesaoujoinstr <- "select bbsfifty.*,species_list.english_common_name,species_list.species_name from bbsfifty left join species_list on bbsfifty.AOU=species_list.aou"

bbsfifty_join_temp <- sqldf(speciesaoujoinstr)

bbsfifty_join <-bbsfifty_join_temp

# Join main table and country lookup to retrieve country

countryjoinstr <- "select bbsfifty_join.*, country_codes.country from bbsfifty_join left join country_codes on bbsfifty_join.countrynum = country_codes.country_code"

bbsfifty_join_temp <- sqldf(countryjoinstr)

bbsfifty_join <- bbsfifty_join_temp

# join main table and state_codes to retrieve state

statejoinstr <- "select bbsfifty_join.*, state_codes.state from bbsfifty_join left join state_codes on bbsfifty_join.cntrystatenum = state_codes.cntrystatenum"

bbsfifty_join_temp <- sqldf(statejoinstr)

bbsfifty_join <- bbsfifty_join_temp

# lookup lat/lon

latlonjoinstr <- "select bbsfifty_join.*, routes.Lati, routes.Longi, routes.Active from bbsfifty_join left join routes on bbsfifty_join.cntrystateroutenum = routes.cntrystateroutenum"

bbsfifty_join_temp <- sqldf(latlonjoinstr)

bbsfifty_join <- bbsfifty_join_temp

# clean up

colnames(bbsfifty_join)

# rename columns

colnames(bbsfifty_join)[1]<-"routedataid"

colnames(bbsfifty_join)[4]<-"route"

colnames(bbsfifty_join)[5]<-"rpid"

colnames(bbsfifty_join)[7]<-"aou"

colnames(bbsfifty_join)[15]<-"lat"

colnames(bbsfifty_join)[16]<-"lon"

colnames(bbsfifty_join)[17]<-"active"


# trim 

trim <- function (x) gsub("^\\s+|\\s+$", "", x)

bbsfifty_join$species_name<-trim(bbsfifty_join$species_name)

# write out

write.csv(bbsfifty_join,"bbsfifty2_partial_processed_2015-02-09.csv",row.names=FALSE)







