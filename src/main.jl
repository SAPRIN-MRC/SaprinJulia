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

#region Set-up execution flags
dostaging = false
dostagebase = false
dostagememberships = false
doreadstatusobs = false
dosocioeconomic = false
dodayextraction = false
dobasedayextraction = false
dopreferredhouseholdextraction = false
doepisodecreation = false
dostataoutput = false
doparentalcoresidency = false
doparentalepisodes = true
# Node specific flags
doAgincourt = true
doDIMAMO = false
doAHRI = true
#endregion

#region Staging
if dostaging
    if dostagebase
        # Individuals
        if doAgincourt
            @info "========== Start readindividuals Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readindividuals("Agincourt")
            @info "========== Finished readindividuals Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start readindividuals DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readindividuals("DIMAMO")
            @info "========== Finished readindividuals DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start readindividuals AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readindividuals("AHRI")
            @info "========== Finished readindividuals AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        #Locations
        if doAgincourt
            @info "========== Start readlocations Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readlocations("Agincourt")
            @info "========== Finished readlocations Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start readlocations DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readlocations("DIMAMO")
            @info "========== Finished readlocations DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start readlocations AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readlocations("AHRI")
            @info "========== Finished readlocations at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        #Individual Residencies
        if doAgincourt
            @info "========== Start readresidences Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readresidences("Agincourt")
            @info "========== Finished readresidences Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end 
        if doDIMAMO
            @info "========== Start readresidences DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readresidences("DIMAMO")
            @info "========== Finished readresidences DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start readresidences AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readresidences("AHRI")
            @info "========== Finished readresidences AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAgincourt
            @info "========== Start readhouseholds Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholds("Agincourt")
            @info "========== Finished readhouseholds Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end 
        if doDIMAMO
            @info "========== Start readhouseholds DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholds("DIMAMO")
            @info "========== Finished readhouseholds DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start readhouseholds AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholds("AHRI")
            @info "========== Finished readhouseholds at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAgincourt
            @info "========== Start readhouseholdmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholdmemberships("Agincourt")
            @info "========== Finished readhouseholdmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start readhouseholdmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholdmemberships("DIMAMO")
            @info "========== Finished readhouseholdmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start readhouseholdmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholdmemberships("AHRI")
            @info "========== Finished readhouseholdmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
    end #dostagebase
    if dostagememberships
        if doAgincourt
            @info "========== Start readindividualmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readindividualmemberships("Agincourt")
            @info "========== Finished readindividualmemberships Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO 
            @info "========== Start readindividualmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readindividualmemberships("DIMAMO")
            @info "========== Finished readindividualmemberships DIMAMO at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start readindividualmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readindividualmemberships("AHRI")
            @info "========== Finished readindividualmemberships AHRI at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
    end #dostagememberships
    if doreadstatusobs
        if doAgincourt
            @info "========== Start readeducationstatuses Agincourt at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readeducationstatuses("Agincourt")
            arrowtostatar(stagingpath("Agincourt"), "EducationStatuses", "EducationStatuses")
            runstata("education.do", settings.Version, "Agincourt", joinpath(stagingpath("Agincourt"), "EducationStatuses.dta"))
             @info "========== Finished Agincourt readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readeducationstatuses("DIMAMO")
            arrowtostatar(stagingpath("DIMAMO"), "EducationStatuses", "EducationStatuses")
            runstata("education.do", settings.Version, "DIMAMO", joinpath(stagingpath("DIMAMO"), "EducationStatuses.dta"))
            @info "========== Finished DIMAMO readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readeducationstatuses("AHRI")
            arrowtostatar(stagingpath("AHRI"), "EducationStatuses", "EducationStatuses")
            runstata("education.do", settings.Version, "AHRI", joinpath(stagingpath("AHRI"), "EducationStatuses.dta"))
            @info "========== Finished AHRI readeducationstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAgincourt
            @info "========== Start Agincourt readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readmaritalstatuses("Agincourt")
            arrowtostatar(stagingpath("Agincourt"), "MaritalStatus", "MaritalStatus")
            runstata("marital.do", settings.Version, "Agincourt", joinpath(stagingpath("Agincourt"), "MaritalStatus.dta"))
            @info "========== Finished Agincourt readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readmaritalstatuses("DIMAMO")
            arrowtostatar(stagingpath("DIMAMO"), "MaritalStatus", "MaritalStatus")
            runstata("marital.do", settings.Version, "DIMAMO", joinpath(stagingpath("DIMAMO"), "MaritalStatus.dta"))
            @info "========== Finished DIMAMO readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readmaritalstatuses("AHRI")
            arrowtostatar(stagingpath("AHRI"), "MaritalStatus", "MaritalStatus")
            runstata("marital.do", settings.Version, "AHRI", joinpath(stagingpath("AHRI"), "MaritalStatus.dta"))
            @info "========== Finished AHRI readmaritalstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAgincourt
            @info "========== Start Agincourt readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readlabourstatuses("Agincourt")
            arrowtostatar(stagingpath("Agincourt"), "LabourStatus", "LabourStatus")
            runstata("labour.do", settings.Version, "Agincourt", joinpath(stagingpath("Agincourt"), "LabourStatus.dta"))
            @info "========== Finished Agincourt readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readlabourstatuses("DIMAMO")
            arrowtostatar(stagingpath("DIMAMO"), "LabourStatus", "LabourStatus")
            runstata("labour.do", settings.Version, "DIMAMO", joinpath(stagingpath("DIMAMO"), "LabourStatus.dta"))
            @info "========== Finished DIMAMO readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readlabourstatuses("AHRI")
            arrowtostatar(stagingpath("AHRI"), "LabourStatus", "LabourStatus")
            runstata("labour.do", settings.Version, "AHRI", joinpath(stagingpath("AHRI"), "LabourStatus.dta"))
            @info "========== Finished AHRI readlabourstatuses at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
    end #doreadstatusobs
    if dosocioeconomic
        if doAgincourt
            @info "========== Start Agincourt readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholdsocioeconomic("Agincourt")
            arrowtostatar(stagingpath("Agincourt"), "AssetStatusRaw", "AssetStatusRaw")
            runstata("assets.do", settings.Version, "Agincourt", joinpath(stagingpath("Agincourt"), "AssetStatusRaw.dta"))
            arrowtostatar(stagingpath("Agincourt"), "SocioEconomicRaw", "SocioEconomicRaw")
            runstata("socioeconomic.do", settings.Version, "Agincourt", joinpath(stagingpath("Agincourt"), "SocioEconomicRaw.dta"))
            @info "========== Finished Agincourt readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholdsocioeconomic("DIMAMO")
            arrowtostatar(stagingpath("DIMAMO"), "AssetStatusRaw", "AssetStatusRaw")
            runstata("assets.do", settings.Version, "DIMAMO", joinpath(stagingpath("DIMAMO"), "AssetStatusRaw.dta"))
            arrowtostatar(stagingpath("DIMAMO"), "SocioEconomicRaw", "SocioEconomicRaw")
            runstata("socioeconomic.do", settings.Version, "DIMAMO", joinpath(stagingpath("DIMAMO"), "SocioEconomicRaw.dta"))
            @info "========== Finished DIMAMO readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            readhouseholdsocioeconomic("AHRI")
            arrowtostatar(stagingpath("AHRI"), "AssetStatusRaw", "AssetStatusRaw")
            runstata("assets.do", settings.Version, "AHRI", joinpath(stagingpath("AHRI"), "AssetStatusRaw.dta"))
            arrowtostatar(stagingpath("AHRI"), "SocioEconomicRaw", "SocioEconomicRaw")
            runstata("socioeconomic.do", settings.Version, "AHRI", joinpath(stagingpath("AHRI"), "SocioEconomicRaw.dta"))
            @info "========== Finished AHRI readhouseholdsocioeconomic at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            flush(io)
        end
    end #dosocioeconomic
end #dostaging
#endregion
#
#region Day Extraction
if dodayextraction
    if dobasedayextraction
#region Residency Days
        if doAgincourt
            @info "========== Start Agincourt extractresidencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extractresidencydays("Agincourt")
            d = now()-t
            @info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO extractresidencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extractresidencydays("DIMAMO")
            d = now()-t
            @info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAHRI
        @info "========== Start AHRI extract residency days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extractresidencydays("AHRI")
            d = now()-t
            @info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
    #endregion
    #region Household Residency Days
        if doAgincourt
            @info "========== Start Agincourt extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extracthhresidencydays("Agincourt")
            d = now()-t
            @info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extracthhresidencydays("DIMAMO")
            d = now()-t
            @info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI extract household residencydays at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extracthhresidencydays("AHRI")
            d = now()-t
            @info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
    #endregion
    #region Household Membership Days
        if doAgincourt
            @info "========== Start Agincourt extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extractmembershipdays("Agincourt")
            d = now()-t
            @info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extractmembershipdays("DIMAMO")
            d = now()-t
            @info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI extract household membership days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            extractmembershipdays("AHRI")
            d = now()-t
            @info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
#endregion
    end #dobasedayextraction
#region Preferred Household Days
    if dopreferredhouseholdextraction
        if doAgincourt
            @info "========== Start Agincourt residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            preferredhousehold("Agincourt")
            d = now()-t
            @info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            preferredhousehold("DIMAMO")
            d = now()-t
            @info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI residency days at preferred household at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            preferredhousehold("AHRI")
            d = now()-t
            @info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
    #endregion
    #region Set Residency Flags
        if doAgincourt
            @info "========== Start Agincourt set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            setresidencyflags("Agincourt")
            d = now()-t
            @info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doDIMAMO
            @info "========== Start DIMAMO set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            setresidencyflags("DIMAMO")
            d = now()-t
            @info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI set residency flags at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            setresidencyflags("AHRI")
            d = now()-t
            @info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
    #endregion
    #region Set Individual Attributes
        if doDIMAMO
            @info "========== Start DIMAMO get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            addindividualattributes("DIMAMO")
            d = now()-t
            @info "DIMAMO extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAgincourt
            @info "========== Start Agincourt get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            addindividualattributes("Agincourt")
            d = now()-t
            @info "Agincourt extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
        if doAHRI
            @info "========== Start AHRI get individual attributes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
            t = now()
            addindividualattributes("AHRI")
            d = now()-t
            @info "AHRI extraction complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
            flush(io)
        end
    end #dopreferredhouseholdextraction
#endregion
end #dodayextraction
#endregion
#region Episode Creation
if doepisodecreation
#region Basic Episodes
    if doAgincourt
        @info "========== Start Agincourt create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        basicepisodes("Agincourt")
        d = now()-t
        @info "Agincourt basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doDIMAMO
        @info "========== Start DIMAMO create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        basicepisodes("DIMAMO")
        d = now()-t
        @info "DIMAMO basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI create basic episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        basicepisodes("AHRI")
        d = now()-t
        @info "AHRI basic episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    # Episode QA
    if doAgincourt
        @info "========== Start Agincourt do basic episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        basicepisodeQA("Agincourt")
        d = now()-t
        @info "Agincourt basic episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doDIMAMO
        @info "========== Start DIMAMO basic episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        basicepisodeQA("DIMAMO")
        d = now()-t
        @info "DIMAMO basic episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI do basic episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        basicepisodeQA("AHRI")
        d = now()-t
        @info "AHRI basic episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
    end
#endregion
#region YrAge Episodes
    if doDIMAMO    
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
    end
    if doAgincourt
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
    end
    if doAHRI
        @info "========== Start AHRI create YrAge episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yrage_episodes("AHRI")
        d = now()-t
        @info "AHRI YrAge episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)

        @info "========== Start AHRI YrAge episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yrage_episodeQA("AHRI")
        d = now()-t
        @info "AHRI YrAge episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
#endregion
#region YrAgeDel Episodes
    if doDIMAMO
        @info "========== Start DIMAMO read pregnancies at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        readpregnancies("DIMAMO")
        d = now()-t
        @info "DIMAMO read pregnancies complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start DIMAMO create delivery days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        deliverydays("DIMAMO")
        d = now()-t
        @info "DIMAMO create delivery days complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start DIMAMO create YrAgeDel episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedel_episodes("DIMAMO")
        d = now()-t
        @info "DIMAMO YrAgeDel episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start DIMAMO YrAgeDel episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedel_episodeQA("DIMAMO")
        d = now()-t
        @info "DIMAMO YrAgeDel episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAgincourt
        @info "========== Start Agincourt read pregnancies at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        readpregnancies("Agincourt")
        d = now()-t
        @info "Agincourt read pregnancies complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start Agincourt create delivery days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        deliverydays("Agincourt")
        d = now()-t
        @info "Agincourt create delivery days complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start Agincourt create YrAgeDel episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedel_episodes("Agincourt")
        d = now()-t
        @info "Agincourt YrAgeDel episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start Agincourt YrAgeDel episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedel_episodeQA("Agincourt")
        d = now()-t
        @info "Agincourt YrAgeDel episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI read pregnancies at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        readpregnancies("AHRI")
        d = now()-t
        @info "AHRI read pregnancies complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start AHRI create delivery days at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        deliverydays("AHRI")
        d = now()-t
        @info "AHRI create delivery days complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start AHRI create YrAgeDel episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedel_episodes("AHRI")
        d = now()-t
        @info "AHRI YrAgeDel episode creation complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start AHRI YrAgeDel episodes QA at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedel_episodeQA("AHRI")
        d = now()-t
        @info "AHRI YrAgeDel episode QA complete $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    #endregion
end #doepisodecreation
#endregion
#region Stata output
if dostataoutput
    if doAgincourt
        @info "========== Start Agincourt output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("Agincourt"), "SurveillanceEpisodesBasic_batched", "SurveillanceEpisodesBasic")
        runstata("label_basicepisodes.do",settings.Version, "Agincourt", joinpath(episodepath("Agincourt"),"SurveillanceEpisodesBasic.dta"))
        d = now()-t
        @info "Agincourt output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doDIMAMO
        @info "========== Start DIMAMO output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("DIMAMO"), "SurveillanceEpisodesBasic_batched", "SurveillanceEpisodesBasic")
        runstata("label_basicepisodes.do",settings.Version, "DIMAMO", joinpath(episodepath("DIMAMO"),"SurveillanceEpisodesBasic.dta"))
        d = now()-t
        @info "DIMAMO output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("AHRI"), "SurveillanceEpisodesBasic_batched", "SurveillanceEpisodesBasic")
        runstata("label_basicepisodes.do",settings.Version, "AHRI", joinpath(episodepath("AHRI"),"SurveillanceEpisodesBasic.dta"))
        d = now()-t
        @info "AHRI output basic STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAgincourt
        @info "========== Start Agincourt output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("Agincourt"), "SurveillanceEpisodesYrAge_batched", "SurveillanceEpisodesYrAge")
        runstata("label_yragepisodes.do", settings.Version, "Agincourt", joinpath(episodepath("Agincourt"),"SurveillanceEpisodesYrAge.dta"))
        d = now()-t
        @info "Agincourt output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doDIMAMO
        @info "========== Start DIMAMO output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("DIMAMO"), "SurveillanceEpisodesYrAge_batched", "SurveillanceEpisodesYrAge")
        runstata("label_yragepisodes.do", settings.Version, "DIMAMO", joinpath(episodepath("DIMAMO"),"SurveillanceEpisodesYrAge.dta"))
        d = now()-t
        @info "DIMAMO output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("AHRI"), "SurveillanceEpisodesYrAge_batched", "SurveillanceEpisodesYrAge")
        runstata("label_yragepisodes.do", settings.Version, "AHRI", joinpath(episodepath("AHRI"),"SurveillanceEpisodesYrAge.dta"))
        d = now()-t
        @info "AHRI output YrAge STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAgincourt
        @info "========== Start Agincourt output YrAgeDel STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("Agincourt"), "SurveillanceEpisodesYrAgeDelivery_batched", "SurveillanceEpisodesYrAgeDelivery")
        runstata("label_yragedeliveryepisodes.do", settings.Version, "Agincourt", joinpath(episodepath("Agincourt"),"SurveillanceEpisodesYrAgeDelivery.dta"))
        d = now()-t
        @info "Agincourt output YrAgeDel STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doDIMAMO
        @info "========== Start DIMAMO output YrAgeDel STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("DIMAMO"), "SurveillanceEpisodesYrAgeDelivery_batched", "SurveillanceEpisodesYrAgeDelivery")
        runstata("label_yragedeliveryepisodes.do", settings.Version, "DIMAMO", joinpath(episodepath("DIMAMO"),"SurveillanceEpisodesYrAgeDelivery.dta"))
        d = now()-t
        @info "DIMAMO output YrAgeDel STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI output YrAgeDel STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        arrowtostatar(episodepath("AHRI"), "SurveillanceEpisodesYrAgeDelivery_batched", "SurveillanceEpisodesYrAgeDelivery")
        runstata("label_yragedeliveryepisodes.do", settings.Version, "AHRI", joinpath(episodepath("AHRI"),"SurveillanceEpisodesYrAgeDelivery.dta"))
        d = now()-t
        @info "AHRI output YrAgeDel STATA episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
end #dostataoutput
#endregion
#region Parent co-residency
if doparentalcoresidency
    if doAgincourt
        @info "========== Start Agincourt mother co-residency at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        mothercoresident("Agincourt")
        d = now()-t
        @info "Agincourt mother co-residency completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start Agincourt father co-residency at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        fathercoresident("Agincourt")
        d = now()-t
        @info "Agincourt father co-residency completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    #
    if doDIMAMO
        @info "========== Start DIMAMO mother co-residency at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        mothercoresident("DIMAMO")
        d = now()-t
        @info "DIMAMO mother co-residency completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start DIMAMO father co-residency at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        fathercoresident("DIMAMO")
        d = now()-t
        @info "DIMAMO father co-residency completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    #
    if doAHRI
        @info "========== Start AHRI mother co-residency at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        mothercoresident("AHRI")
        d = now()-t
        @info "AHRI mother co-residency completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
        #
        @info "========== Start AHRI father co-residency at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        fathercoresident("AHRI")
        d = now()-t
        @info "AHRI father co-residency completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
end 
if doparentalepisodes
    if doAgincourt
        @info "========== Start Agincourt parental episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedelparentalstatus_episodes("Agincourt")
        arrowtostatar(episodepath("Agincourt"), "SurveillanceEpisodesYrAgeDeliveryParents_batched", "SurveillanceEpisodesYrAgeDeliveryParents")
        runstata("label_yragedeliveryparentepisodes.do", settings.Version, "Agincourt", joinpath(episodepath("Agincourt"),"SurveillanceEpisodesYrAgeDeliveryParents.dta"))
        d = now()-t
        @info "Agincourt parental episodes completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doDIMAMO
        @info "========== Start DIMAMO parental episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedelparentalstatus_episodes("DIMAMO")
        arrowtostatar(episodepath("DIMAMO"), "SurveillanceEpisodesYrAgeDeliveryParents_batched", "SurveillanceEpisodesYrAgeDeliveryParents")
        runstata("label_yragedeliveryparentepisodes.do", settings.Version, "DIMAMO", joinpath(episodepath("DIMAMO"),"SurveillanceEpisodesYrAgeDeliveryParents.dta"))
        d = now()-t
        @info "DIMAMO parental episodes completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
    if doAHRI
        @info "========== Start AHRI parental episodes at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))"
        t = now()
        yragedelparentalstatus_episodes("AHRI")
        arrowtostatar(episodepath("AHRI"), "SurveillanceEpisodesYrAgeDeliveryParents_batched", "SurveillanceEpisodesYrAgeDeliveryParents")
        runstata("label_yragedeliveryparentepisodes.do", settings.Version, "AHRI", joinpath(episodepath("AHRI"),"SurveillanceEpisodesYrAgeDeliveryParents.dta"))
        d = now()-t
        @info "AHRI parental episodes completed at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS")) duration $(round(d, Dates.Second))"
        flush(io)
    end
end
#
#endregion
#
#region clean up
global_logger(old_logger)
close(io)
#endregion