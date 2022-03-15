using SAPRINCore
using DataFrames
using Arrow


function convertmaritaltostata(node::String)
    arrowtostatar(node,stagingpath(node),"MaritalStatus","MaritalStatusRaw")
    runstata("marital.do",settings.Version, node, joinpath(stagingpath(node),"MaritalStatusRaw"))
end


convertmaritaltostata("AGINCOURT")
convertmaritaltostata("AHRI")
convertmaritaltostata("DIMAMO")