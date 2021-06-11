"Group day records into basic exposure records"
function basicepisodes(node)
    residentdaybatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "DayDatasetStep02_batched.arrow"))
    hstate = iterate(residentdaybatches)
    i = 1
    while hstate !== nothing
        t = now()
        @info "Node $(node) batch $(i) at $(t)"
        h, hst = hstate
        hd = h |> DataFrame
        e = combine(groupby(hd, [:IndividualId, :Episode]), :Resident => first => :Resident, :LocationId => first => :LocationId, :HouseholdId => first => :HouseholdId, 
                    :Sex => first => :Sex, :DoB => first => :DoB, :DoD => first => :DoD, :MotherId => first => :MotherId, :FatherId => first => :FatherId,
                    :DayDate => minimum => :StartDate, :DayDate => maximum => :EndDate, nrow => :Days, 
                    :Born => maximum => :Born, :Enumeration => maximum => :Enumeration, :InMigration => maximum => :InMigration, :LocationEntry => maximum => :LocationEntry, :ExtResStart => maximum => :ExtResStart, :Participation => maximum => :Participation,
                    :Died => maximum => :Died, :OutMigration => maximum => :OutMigration, :LocationExit => maximum => :LocationExit, :ExtResEnd => maximum => :ExtResEnd, :Refusal => maximum => :Refusal, :LostToFollowUp => maximum => :LostToFollowUp, 
                    :Current => maximum => :Current, :MembershipStart => maximum => :MembershipStart, :MembershipEnd => maximum => :MembershipEnd, :Memberships => maximum => :Memberships, :GapStart => maximum => :Gap)
        filter!(row -> ismissing(row.DoD) || (!ismissing(row.DoD) && (row.StartDate < row.DoD || row.DoB == row.DoD)), e) #Episode start must be less than DoD, unless person born and died on the same day
        episodes = transform(groupby(sort(e, [:IndividualId, :Episode]), [:IndividualId]), nrow => :Episodes)
        insertcols!(episodes, :Delete =>  Int16(0))
        lastIndividual = -1
        iscurrent = Int16(0)
        for row in eachrow(episodes)
            if !ismissing(row.DoD) && row.EndDate > row.DoD
                row.EndDate = row.DoD
                row.Days = Dates.value(row.EndDate - row.StartDate) + 1
                row.Died = Int16(1)
            end
            # Fix Flags
            if !ismissing(row.DoD) && row.DoD == row.EndDate
                row.Died = Int16(1)
            else
                row.Died = Int16(0)
            end
            if row.DoB == row.StartDate
                row.Born = Int16(1)
            else
                row.Born = Int16(0)
            end
            if row.Died == 1 && row.OutMigration == 1
                row.OutMigration = Int16(0)
            end
            if row.Died == 1 && row.LostToFollowUp == 1
                row.LostToFollowUp = Int16(0)
            end
            if row.Born == 1
                row.Enumeration = Int16(0)
                row.InMigration = Int16(0)
                row.LocationEntry = Int16(0)
                row.ExtResStart = Int16(0)
                row.MembershipStart = Int16(1)
            elseif row.Enumeration == 1
                row.InMigration = Int16(0)
                row.LocationEntry = Int16(0)
                row.ExtResStart = Int16(0)
                row.MembershipStart = Int16(1)
            elseif row.InMigration == 1
                row.LocationEntry = Int16(0)
                row.ExtResStart = Int16(0)
            end
            if row.LostToFollowUp == 1 || row.Refusal == 1 || row.Died == 1
                row.MembershipEnd = Int16(1)
            elseif row.Current == 1
                row.MembershipEnd = Int16(0)
            end
            if row.ExtResEnd == 1 && row.LostToFollowUp == 1
                row.ExtResEnd = Int16(0)
            end
            # mark for deletions all episodes after current
            if lastIndividual != row.IndividualId
                iscurrent = row.Current
                lastIndividual = row.IndividualId
            elseif iscurrent == 1
                row.Delete == 1
            else
                iscurrent = row.Current
            end
        end
        filter!(r -> r.Delete == 0, episodes)
        select!(episodes,Not([:Delete,:Episode,:Episodes]))
        e = transform(groupby(sort(episodes,[:IndividualId, :StartDate]), [:IndividualId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
        open(joinpath(basedirectory, node, "Episodes", "SurveillanceEpisodesBasic$(i).arrow"),"w"; lock = true) do io
            Arrow.write(io, e, compress=:zstd)
        end
        hstate = iterate(residentdaybatches, hst)
        @info "Node $(node) batch $(i) completed with $(nrow(e)) episodes after $(round(now()-t, Dates.Second))"
        i = i + 1
    end
    combinebatches(episodepath(node), "SurveillanceEpisodesBasic", i-1)
    return nothing
end