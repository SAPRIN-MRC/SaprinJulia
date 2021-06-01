using Logging
using SAPRINCore
using Dates

#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize=>(100,100))
logger = SimpleLogger(io)
old_logger = global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))" 
flush(io)
#endregion

#region Staging
# Individuals
#
@info "========== Start readindividuals Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividuals("Agincourt")
@info "========== Finished readindividuals Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

@info "========== Start readindividuals DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividuals("DIMAMO")
@info "========== Finished readindividuals DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
@info "========== Start readindividuals AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividuals("AHRI")
@info "========== Finished readindividuals AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#Locations
#=
@info "========== Start readlocations Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readlocations("Agincourt")
flush(io)
#
@info "========== Start readlocations DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readlocations("DIMAMO")
flush(io)

@info "========== Start readlocations AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readlocations("AHRI")
@info "========== Finished readlocations at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#Individual Residencies
# 
@info "========== Start readresidences Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readresidences("Agincourt")
@info "========== Finished readresidences Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
@info "========== Start readresidences DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readresidences("DIMAMO")
@info "========== Finished readresidences DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
@info "========== Start readresidences AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readresidences("AHRI")
@info "========== Finished readresidences AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#
@info "========== Start readhouseholds Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readhouseholds("Agincourt")
@info "========== Finished readhouseholds Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
# 
@info "========== Start readhouseholds DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readhouseholds("DIMAMO")
@info "========== Finished readhouseholds DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

@info "========== Start readhouseholds AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readhouseholds("AHRI")
@info "========== Finished readhouseholds at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
# 
@info "========== Start readhouseholdmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readhouseholdmemberships("Agincourt")
@info "========== Finished readhouseholdmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
# 
@info "========== Start readhouseholdmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readhouseholdmemberships("DIMAMO")
@info "========== Finished readhouseholdmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

@info "========== Start readhouseholdmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readhouseholdmemberships("AHRI")
@info "========== Finished readhouseholdmemberships at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
# 
@info "========== Start readindividualmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividualmemberships("Agincourt", 25000)
@info "========== Finished readindividualmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
# 
@info "========== Start readindividualmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividualmemberships("DIMAMO", 25000)
@info "========== Finished readindividualmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
@info "========== Start readindividualmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividualmemberships("AHRI", 25000)
@info "========== Finished readindividualmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#
readeducationstatuses("Agincourt")
@info "========== Finished Agincourt readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readeducationstatuses("DIMAMO")
@info "========== Finished DIMAMO readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readeducationstatuses("AHRI")
@info "========== Finished AHRI readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readhouseholdsocioeconomic("Agincourt")
@info "========== Finished Agincourt readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readhouseholdsocioeconomic("DIMAMO")
@info "========== Finished DIMAMO readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readhouseholdsocioeconomic("AHRI")
@info "========== Finished AHRI readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readmaritalstatuses("Agincourt")
@info "========== Finished Agincourt readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readmaritalstatuses("DIMAMO")
@info "========== Finished DIMAMO readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readmaritalstatuses("AHRI")
@info "========== Finished AHRI readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readlabourstatuses("Agincourt")
@info "========== Finished Agincourt readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

readlabourstatuses("DIMAMO")
@info "========== Finished DIMAMO readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readlabourstatuses("AHRI")
@info "========== Finished AHRI readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#endregion
#
#region Day Extraction
@info "========== Start Agincourt extractresidencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractresidencydays("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start DIMAMO extractresidencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractresidencydays("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI extract residency days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractresidencydays("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# 
@info "========== Start Agincourt extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extracthhresidencydays("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# 
@info "========== Start DIMAMO extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extracthhresidencydays("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start AHRI extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extracthhresidencydays("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
# 
@info "========== Start Agincourt extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractmembershipdays("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# 
@info "========== Start DIMAMO extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractmembershipdays("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractmembershipdays("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
# 
@info "========== Start Agincourt residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
preferredhousehold("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# 
@info "========== Start DIMAMO residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
preferredhousehold("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
preferredhousehold("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# 
@info "========== Start Agincourt set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
setresidencyflags("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#=
@info "========== Start DIMAMO set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
setresidencyflags("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
=#
@info "========== Start AHRI set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
setresidencyflags("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#=
@info "========== Start DIMAMO get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
addindividualattributes("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
=#
@info "========== Start Agincourt get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
addindividualattributes("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start AHRI get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
addindividualattributes("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#endregion
#region Basic Episodes
#
@info "========== Start Agincourt create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodes("Agincourt")
d = now()-t
@info "Agincourt basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#=
@info "========== Start DIMAMO create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodes("DIMAMO")
d = now()-t
@info "DIMAMO basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
=#
@info "========== Start AHRI create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodes("AHRI")
d = now()-t
@info "AHRI basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# Episode QA
#
@info "========== Start Agincourt do basic episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodeQA("Agincourt")
d = now()-t
@info "Agincourt basic episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#=
@info "========== Start DIMAMO basic episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodeQA("DIMAMO")
d = now()-t
@info "DIMAMO basic episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
=#
@info "========== Start AHRI do basic episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodeQA("AHRI")
d = now()-t
@info "AHRI basic episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
#=
@info "========== Start DIMAMO create YrAge episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
yrage_episodes("DIMAMO")
d = now()-t
@info "DIMAMO YrAge episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start DIMAMO YrAge episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
yrage_episodeQA("DIMAMO")
d = now()-t
@info "DIMAMO YrAge episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
=#
@info "========== Start Agincourt create YrAge episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
yrage_episodes("Agincourt")
d = now()-t
@info "Agincourt YrAge episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start Agincourt YrAge episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
yrage_episodeQA("Agincourt")
d = now()-t
@info "Agincourt YrAge episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start AHRI create YrAge episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
yrage_episodes("AHRI")
d = now()-t
@info "AHRI YrAge episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start AHRI YrAge episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
yrage_episodeQA("AHRI")
d = now()-t
@info "AHRI YrAge episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
#endregion
#
#region Stata output
@info "========== Start Agincourt output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
arrowtostata("Agincourt", "SurveillanceEpisodesBasic_batched", "SurveillanceEpisodesBasic")
d = now()-t
@info "Agincourt output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start DIMAMO output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
arrowtostata("DIMAMO", "SurveillanceEpisodesBasic_batched", "SurveillanceEpisodesBasic")
d = now()-t
@info "DIMAMO output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start AHRI output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
arrowtostata("AHRI", "SurveillanceEpisodesBasic_batched", "SurveillanceEpisodesBasic")
d = now()-t
@info "AHRI output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start Agincourt output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
arrowtostata("Agincourt", "SurveillanceEpisodesYrAge_batched", "SurveillanceEpisodesYrAge")
d = now()-t
@info "Agincourt output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start DIMAMO output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
arrowtostata("DIMAMO", "SurveillanceEpisodesYrAge_batched", "SurveillanceEpisodesYrAge")
d = now()-t
@info "DIMAMO output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
#
@info "========== Start AHRI output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
arrowtostata("AHRI", "SurveillanceEpisodesYrAge_batched", "SurveillanceEpisodesYrAge")
d = now()-t
@info "AHRI output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
=#

#endregion
#region clean up
global_logger(old_logger)
close(io)
#endregion