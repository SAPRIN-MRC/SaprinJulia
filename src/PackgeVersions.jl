using Pkg

cd(joinpath(DEPOT_PATH[1], "registries", "General")) do
    deps = Pkg.dependencies()
    registry = Pkg.TOML.parse(read("Registry.toml", String))
    general_pkgs = registry["packages"]

    constrained = Dict{String, Tuple{VersionNumber,VersionNumber}}()
    for (uuid, dep) in deps
        suuid = string(uuid)
        dep.is_direct_dep || continue
        dep.version === nothing && continue
        haskey(general_pkgs, suuid) || continue
        pkg_meta = general_pkgs[suuid]
        pkg_path = joinpath(pkg_meta["path"], "Versions.toml")
        versions = Pkg.TOML.parse(read(pkg_path, String))
        newest = maximum(VersionNumber.(keys(versions)))
        if newest > dep.version
            constrained[dep.name] = (dep.version, newest)
        end
    end

    return constrained
end