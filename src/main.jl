using Logging
using SAPRINCore
using Dates

#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize=>(100,100))
logger = SimpleLogger(io)
global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))" 
flush(io)
#endregion

#region Staging
# Individuals
#=
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
#endregion
=#
@time readindividualmemberships("Agincourt", 10000)
@info "========== Finished readindividualmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
@time readindividualmemberships("DIMAMO", 10000)
@info "========== Finished readindividualmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
@time readindividualmemberships("AHRI", 10000)
@info "========== Finished readindividualmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
flush(io)
#region clean up
close(io)
#endregion