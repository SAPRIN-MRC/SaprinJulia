using SAPRINCore
using DataFrames
using Arrow


function convertlabourtostata(node::String)
    arrowtostatar(node,stagingpath(node),"LabourStatus","LabourStatusRaw")
    runstata("labour.do",settings.Version, node, joinpath(stagingpath(node),"LabourStatusRaw"))
end


convertlabourtostata("AGINCOURT")
convertlabourtostata("AHRI")
convertlabourtostata("DIMAMO")