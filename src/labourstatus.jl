using SAPRINCore
using DataFrames
using Arrow


function convertlabourtostata(node::String)
    arrowtostatar(node,stagingpath(node),"LabourStatus","LabourStatusRaw")
    runstata("labour.do",settings.Version, node, joinpath(stagingpath(node),"LabourStatusRaw"))
end

function converteducationtostata(node::String)
    arrowtostatar(node,stagingpath(node),"EducationStatuses","EducationStatusRaw")
#    runstata("labour.do",settings.Version, node, joinpath(stagingpath(node),"EducationStatusRaw"))
end

# convertlabourtostata("AGINCOURT")
# convertlabourtostata("AHRI")
# convertlabourtostata("DIMAMO")

converteducationtostata("AHRI")
converteducationtostata("DIMAMO")