using SAPRINCore


nodes = ["Agincourt", "DIMAMO", "AHRI"]
for node in nodes
    minId, maxId, numbatches = individualbatch(node)
    filename = "DayDatasetStep01"
    if isfile(joinpath(dayextractionpath(node), "$(filename)1.zjls"))
        deletebatchfiles(dayextractionpath(node), filename, numbatches)
    end
end