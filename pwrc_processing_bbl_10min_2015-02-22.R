# Set working directory

setwd("C:/Users/derek/R/pwrcbbl")

# Import datasets

bblspecies <- read.csv("~/R/usgs-bbl-2015-03/bison_10M_blk/bison_10M_blk.csv", header=TRUE)

specieslu <- read.csv("~/R/usgs-bbl-2015-03/species_lookup.csv")

stctylu <- read.csv("~/R/usgs-bbl-2015-03//cntry_state_cnty_lookup.csv")

# Review data

View(bblspecies)

View(specieslu)

View(stctylu)

# drop columns

bblspecies$X<-NULL

specieslu$X...<-NULL

stctylu$X...<-NULL

# Add sqldf library

install.packages("sqldf")

library("sqldf", lib.loc="/Library/Frameworks/R.framework/Versions/3.1/Resources/library")

# Add species names to bblspecies dataframe

# SPECIES_ID in bblspecies is our key for retrieving scientific/common name

speciesjoinstr <- "select bblspecies.*, specieslu.SCI_NAME, specieslu.SPECIES_NAME, specieslu.ALPHA_CODE from bblspecies left join specieslu on bblspecies.SPECIES_ID = specieslu.SPECIES_ID"

bblspecies_join <- sqldf(speciesjoinstr)

# change date format ex. "12/14/98" >> "1998-12-14"

bblspecies_join_temp$occurrence_date<-as.Date(strptime(bblspecies_join_temp$occurrence_date,'%Y-%m-%d'))

# extract year from date

bblspecies_join_temp$year<-format(bblspecies_join_temp$occurrence_date, "%Y") 

# Add geographic information from state county lookup

# Clean up code fields

# County code should be 3 character string

stctylu$COUNTY_CODE<-sprintf("%03d",stctylu$COUNTY_CODE)

bblspecies_join$COUNTY_CODE<-sprintf("%03d",bblspecies_join$COUNTY_CODE)

# State code should be 2 character string

stctylu$STATE_CODE<-sprintf("%02d",stctylu$STATE_CODE)

bblspecies_join$STATE_CODE<-sprintf("%02d",bblspecies_join$STATE_CODE)

# Concatenate state and county code to get 5 character FIPS

bblspecies_join$FIPS <- paste(bblspecies_join$STATE_CODE, bblspecies_join$COUNTY_CODE, sep='')

bblspecies_join$CFIPS <- paste(bblspecies_join$COUNTRY_CODE, bblspecies_join$FIPS, sep='')

stctylu$FIPS <- paste(stctylu$STATE_CODE, stctylu$COUNTY_CODE, sep='')

stctylu$CFIPS <- paste(stctylu$COUNTRY_CODE, stctylu$FIPS, sep='')

# Join bblspecies_join and state lookup to retrieve state county

stcntyjoinstr <- "select bblspecies_join.*, stctylu.STATE_NAME, stctylu.COUNTY_NAME, stctylu.COUNTY_DESCRIPTION from bblspecies_join left join stctylu on bblspecies_join.CFIPS = stctylu.CFIPS"

bblspecies_join_temp <- sqldf(stcntyjoinstr)

# if join returns same number of rows as original then commit final

bblspecies_join <- bblspecies_join_temp

# remove temp dataframes

rm(bblspecies_join_temp)

rm(bblspecies)

# Clean up headers and drop columns

# change column header name to match BISON schema

colnames(bblspecies_join)

colnames(bblspecies_join)[2]<-"latitude"

colnames(bblspecies_join)[3]<-"longitude"

colnames(bblspecies_join)[5]<-"iso_country_code"

colnames(bblspecies_join)[8]<-"provided_fips"

colnames(bblspecies_join)[9]<-"clean_provided_scientific_name"

colnames(bblspecies_join)[10]<-"provided_common_name"

colnames(bblspecies_join)[13]<-"occurrence_date"

colnames(bblspecies_join)[15]<-"provided_state_name"

colnames(bblspecies_join)[16]<-"provided_county_name"

# view column names

colnames(bblspecies_join)

# drop columns not used in final BISON data load

bblspecies_join$SPECIES_ID<-NULL

bblspecies_join$Banding.Date<-NULL

bblspecies_join$STATE_CODE<-NULL

bblspecies_join$COUNTY_CODE<-NULL

bblspecies_join$ALPHA_CODE<-NULL

bblspecies_join$CFIPS<-NULL

bblspecies_join$COUNTY_DESCRIPTION<-NULL

# add BISON columns and order using template and datamerge library

## create data columns for BISON ingest by running external R script

source("Create_BISON_DataFrame_43F.R") 

# create subset to work against

bblspecies_subset<-bblspecies_join

# use template to order data columns 

bison_final_bbl_state<-version.merge(bsn43f, bblspecies_subset, add.rows=TRUE, add.cols=TRUE, add.values=TRUE, verbose=TRUE)

## create data columns for BISON ingest

row.names(bsn43f)<-NULL #drop row names

# bblspecies_join_temp$clean_provided_scientific_name<-"" # field 1

bblspecies_join_temp$itis_common_name<-"" # field 2

bblspecies_join_temp$itis_tsn<-"" # field  3

# bblspecies_join_temp$basis_of_record<-"observation" # field 4

# bblspecies_join_temp$occurrence_date<-"" # field 5

# bblspecies_join_temp$year<-"" # field 6

bblspecies_join_temp$provider<-"BISON" # field 7

bblspecies_join_temp$provider_url<-"http://bison.usgs.ornl.gov" # field 8

bblspecies_join_temp$resource<-"USGS PWRC - Bird Banding Lab - US Records 10min Block" # field 9 

bblspecies_join_temp$resource_url<-"http://www.pwrc.usgs.gov/bbl" # field 10

bblspecies_join_temp$occurrence_url<-"" # field 11

bblspecies_join_temp$catalog_number<-"" # field 12

bblspecies_join_temp$collector<-"" # field 13

bblspecies_join_temp$collector_number<-"" # field 14

bblspecies_join_temp$valid_accepted_scientific_name<-"" # field 15

bblspecies_join_temp$valid_accepted_tsn<-"" # field 16

bblspecies_join_temp$provided_scientific_name<-bblspecies_join_temp$clean_provided_scientific_name # field 17

bblspecies_join_temp$provided_tsn<-"" # field 18

# bblspecies_join_temp$latitude<-"" # field 19

# bblspecies_join_temp$longitude<-"" # field 20

bblspecies_join_temp$verbatim_elevation<-"" # field 21

bblspecies_join_temp$verbatim_depth<-"" # field 22

bblspecies_join_temp$calculated_county_name<-"" # field 23

bblspecies_join_temp$calculated_fips<-"" # field 24

bblspecies_join_temp$calculated_state_name<-"" # field 25

bblspecies_join_temp$centroid<-"10 Minute Block" # field 26

# bblspecies_join_temp$provided_county_name<-"" # field 27

bblspecies_join_temp$provided_fips<-"" # field 28

# bblspecies_join_temp$provided_state_name<-"" # field 29

bblspecies_join_temp$thumb_url<-"" # field 30

bblspecies_join_temp$associated_media<-"" # field 31

bblspecies_join_temp$associated_references<-"" # field 32

bblspecies_join_temp$general_comments<-"" # field 33

bblspecies_join_temp$id<-"" # field 34 populate id with row number

bblspecies_join_temp$provider_id<-"" # field 35

bblspecies_join_temp$resource_id<-"" # field 36

# bblspecies_join_temp$provided_common_name<-"" # field 37

bblspecies_join_temp$provided_kingdom<-"Animalia" # field 38

bblspecies_join_temp$geodetic_datum<-"" # field 39

bblspecies_join_temp$coordinate_precision<-"" # field 40

bblspecies_join_temp$coordinate_uncertainty<-"" # field 41

bblspecies_join_temp$verbatim_locality<-"" # field 42

# bblspecies_join_temp$iso_country_code<-"" # field 43

# use rbind to order data columns - bsn43f is master order

bblspecies_join_temp <- rbind(bsn43f,bblspecies_join_temp)

bblspecies_join_temp <- bblspecies_join_temp[-1,] # remove first row

# subset for review

uspecies_bbl<-unique(bblspecies_join_temp$clean_provided_scientific_name)
write.table(uspecies_bbl, file = "bison_bbl_10min_uspecies_2015-02-23.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(uspecies_bbl)

bbl_top50<-head(bblspecies_join_temp)
write.table(bbl_top50, file = "bison_bbl_10min_top50_2015-02-23.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(bbl_top50)

bbl_bottom50<-tail(bblspecies_join_temp)
write.table(bbl_bottom50, file = "bison_bbl_10min_bottom_50_2015-02-23.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(bbl_bottom50)

bbl_subset100000<-bblspecies_join_temp[1:100000,]
write.table(bbl_subset100000, file = "bison_bbl_10min_100k_2015-02-23.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
rm(bbl_subset100000)

# write out final bbl data

write.table(bblspecies_join_temp, file = "bison_bbl_10min_ordered_final_2015-02-23.txt", append = FALSE, quote = FALSE, sep= "\t", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)

## ----- ##

# alternate method for ordering columns and checking for missing

# create subset to work against

bblspecies_subset<-bblspecies_join

# use template to order data columns 

bison_final_bbl_state<-version.merge(bsn43f, bblspecies_subset, add.rows=TRUE, add.cols=TRUE, add.values=TRUE, verbose=TRUE)

# method(2) detect add remaining columns

colnamelist<-names(bsn43f)

missingcols<-setdiff(colnamelist,names(bblspecies_join))

bblspecies_join_temp[,missingcols]<-""

bblspecies_join_temp<-bblspecies_join_temp[colnamelist]

# sort

bison_columns<-fwsrefuges[1,]

##--Date Format--##

bblspecies_join_temp$occurrence_date<-as.numeric(bblspecies_join_temp$occurrence_date)

bblspecies_join_temp$occurrence_date<-as.Date(bblspecies_join_temp$occurrence_date, origin="1970-01-01")

bblspecies_join_temp$occurrence_date<-format(bblspecies_join_temp$occurrence_date)


