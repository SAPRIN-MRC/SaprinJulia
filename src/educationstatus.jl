using SAPRINCore
using DataFrames
using Arrow


function converteducationtostata(node::String)
    arrowtostatar(node,stagingpath(node),"EducationStatuses","EducationStatusesRaw")
    runstata("education.do",settings.Version, node, joinpath(stagingpath(node),"EducationStatusesRaw"))
end


converteducationtostata("AGINCOURT")
converteducationtostata("AHRI")
converteducationtostata("DIMAMO")