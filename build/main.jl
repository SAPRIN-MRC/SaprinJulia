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
#=
#region Staging
# Individuals
@info "========== Start readindividuals at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
readindividuals("Agincourt")
flush(io)

readindividuals("DIMAMO")
flush(io)

readindividuals("AHRI")
@info "========== Finished readindividuals at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#Locations
readlocations("Agincourt")
flush(io)

readlocations("DIMAMO")
flush(io)

readlocations("AHRI")
@info "========== Finished readlocations at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#Individual Residencies
readresidences("Agincourt")
@info "========== Finished readresidences Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

readresidences("DIMAMO")
@info "========== Finished readresidences DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

readresidences("AHRI")
@info "========== Finished readresidences AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readhouseholds("Agincourt")
flush(io)

readhouseholds("DIMAMO")
flush(io)

readhouseholds("AHRI")
@info "========== Finished readhouseholds at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readhouseholdmemberships("Agincourt")
flush(io)

readhouseholdmemberships("DIMAMO")
flush(io)

readhouseholdmemberships("AHRI")
@info "========== Finished readhouseholdmemberships at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
readindividualmemberships("Agincourt", 25000)
@info "========== Finished readindividualmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

readindividualmemberships("DIMAMO", 25000)
@info "========== Finished readindividualmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)

readindividualmemberships("AHRI", 25000)
@info "========== Finished readindividualmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
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
#region Day Extraction
@info "========== Start Agincourt extractresidencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractresidencydays("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start DIMAMO extractresidencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractresidencydays("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI extract residency days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractresidencydays("AHRI")
d = now()-t
@info "AHRI extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)
@info "========== Start Agincourt extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extracthhresidencydays("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start DIMAMO extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extracthhresidencydays("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extracthhresidencydays("AHRI")
d = now()-t
@info "AHRI extraction complete $(now()) duration $(round(d, Dates.Second))"
@info "========== Start Agincourt extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractmembershipdays("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start DIMAMO extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractmembershipdays("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
extractmembershipdays("AHRI")
d = now()-t
@info "AHRI extraction complete $(now()) duration $(round(d, Dates.Second))"
@info "========== Start Agincourt residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
preferredhousehold("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start DIMAMO residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
preferredhousehold("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
preferredhousehold("AHRI")
d = now()-t
@info "AHRI extraction complete $(now()) duration $(round(d, Dates.Second))"
@info "========== Start Agincourt set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
setresidencyflags("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start DIMAMO set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
setresidencyflags("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(now()) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start AHRI set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
setresidencyflags("AHRI")
d = now()-t
@info "AHRI extraction complete $(now()) duration $(round(d, Dates.Second))"

@info "========== Start DIMAMO get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
addindividualattributes("DIMAMO")
d = now()-t
@info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)

@info "========== Start Agincourt get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
addindividualattributes("Agincourt")
d = now()-t
@info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
@info "========== Start AHRI get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
addindividualattributes("AHRI")
d = now()-t
@info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
#endregion

#region Basic Episodes
# @info "========== Start Agincourt create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
# t = now()
# basicepisodes("Agincourt")
# d = now()-t
# @info "Agincourt basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
# flush(io)
=#
@info "========== Start DIMAMO create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
t = now()
basicepisodes("DIMAMO")
d = now()-t
@info "DIMAMO basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
flush(io)
# @info "========== Start AHRI create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
# t = now()
# basicepisodes("AHRI")
# d = now()-t
# @info "AHRI basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
#endregion
#region clean up
global_logger(old_logger)
close(io)
#endregion