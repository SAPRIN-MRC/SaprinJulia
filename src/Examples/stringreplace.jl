s = ["this is a #test#", "this is a second #test#"]
for i in 1:length(s)
   s[i] = replace(s[i], "#test#" => "replaced")
end
fname = tempname() * ".do"
open(fname,"w") do f
    for i in 1:length(s)
        println(f, s[i])
    end
end
fname