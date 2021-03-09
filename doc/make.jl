using Documenter
using SAPRINCore

Documenter.makedocs(root = "./", 
                    source = "src",
                    build = "build",
                    clean = true,
                    doctest = true,
                    modules = [SAPRINCore],
                    repo = "",
                    highlightsig = true,
                    sitename = "SAPRINCore Documentation",
                    expandfirst = [],
                    pages = [
                        "Index" => "index.md"
                    ])

