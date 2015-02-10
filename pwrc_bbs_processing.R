# joined tables bbsfifty1

# working on bbsfifty2

# format country code in main table

bbsfifty1$countrynum<-sprintf("%03d",bbsfifty1$countrynum)

# format state code in main table

bbsfifty1$statenum<-sprintf("%02d",bbsfifty1$statenum)

# format route code in main table

bbsfifty1$Route<-sprintf("%03d",bbsfifty1$Route)

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

bbsfifty1$cntrystatenum <- paste(bbsfifty1$countrynum, bbsfifty1$statenum, sep='')

# concatenate country, state, route code in main table to create lookup field for country/state/route lat/lon

bbsfifty1$cntrystateroutenum <- paste(bbsfifty1$cntrystatenum, bbsfifty1$Route, sep='')

# concatenate country, state in state_codes table to create lookup field for state

state_codes$cntrystatenum <- paste(state_codes$countrynum, state_codes$regioncode, sep='')

# concatenate country and state code in Route table to create lookup field for country/state

routes$cntrystatenum <- paste(routes$countrynum, routes$statenum, sep='')

# concatenate country, state, route code in Route table to create lookup field for country/state/route lat/lon

routes$cntrystateroutenum <- paste(routes$cntrystatenum, routes$Route, sep='')

# format aou code in main table

bbsfifty1$AOU<-sprintf("%05d",bbsfifty1$AOU)

# format aou code in species_list table

species_list$aou<-sprintf("%05d",species_list$aou)

# lookup species scientific name

speciesaoujoinstr <- "select bbsfifty1.*,species_list.english_common_name,species_list.species_name from bbsfifty1 left join species_list on bbsfifty1.AOU=species_list.aou"

bbsfifty1_join_temp <- sqldf(speciesaoujoinstr)

bbsfifty1_join <-bbsfifty1_join_temp

# Join main table and country lookup to retrieve country

countryjoinstr <- "select bbsfifty1_join.*, country_codes.country from bbsfifty1_join left join country_codes on bbsfifty1_join.countrynum = country_codes.country_code"

bbsfifty1_join_temp <- sqldf(countryjoinstr)

bbsfifty1_join <- bbsfifty1_join_temp

# join main table and state_codes to retrieve state

statejoinstr <- "select bbsfifty1_join.*, state_codes.state from bbsfifty1_join left join state_codes on bbsfifty1_join.cntrystatenum = state_codes.cntrystatenum"

bbsfifty1_join_temp <- sqldf(statejoinstr)

bbsfifty1_join <- bbsfifty1_join_temp

# lookup lat/lon

latlonjoinstr <- "select bbsfifty1_join.*, routes.Lati, routes.Longi, routes.Active from bbsfifty1_join left join routes on bbsfifty1_join.cntrystateroutenum = routes.cntrystateroutenum"

bbsfifty1_join_temp <- sqldf(latlonjoinstr)

bbsfifty1_join <- bbsfifty1_join_temp

# add observations from all stops

bbsfifty_temp$stopstotalcount <- rowSums(bbsfifty_temp[, c(8:57)])

# concatenate into counts for each stop

bbsfifty_temp<-bbsfifty

bbsfifty_temp$stopcount <- do.call(paste, c(bbsfifty_temp[8:57], sep=";"))

# clean up

# trim 

trim <- function (x) gsub("^\\s+|\\s+$", "", x)

bsnfiaprivfinal$clean_provided_scientific_name<-trim(bsnfiaprivfinal$clean_provided_scientific_name)

# write out

write.csv(bbsfifty_temp,"bbsfifty2_partial_processed_2015-02-09.csv",row.names=FALSE)







