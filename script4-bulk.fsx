#r "System.Data.dll"
#r "System.Data.Linq.dll"
#r "FSharp.Data.TypeProviders.dll"
#r @"packages\Newtonsoft.Json\lib\net45\Newtonsoft.Json.dll"
#r @"packages\FSharp.Data\lib\net40\FSharp.Data.dll"

open System
open System.Data
open System.Data.Linq
open System.IO
open Microsoft.FSharp.Data.TypeProviders
open Microsoft.FSharp.Linq
open FSharp.Data
open FSharp.Data.HttpRequestHeaders
open Newtonsoft.Json

[<Literal>]
type DbSchema = SqlDataConnection<Conn>
type Dbc = DbSchema.ServiceTypes.SimpleDataContextTypes.TotallyMoney_CreditCards

let (db:Dbc) = DbSchema.GetDataContext()
let srlz x = JsonConvert.SerializeObject(x)
let sub n (v:String) = v.Substring(0, n)
let writeFile text i =
    let file = sprintf "%s\\out\\out-%i.txt" __SOURCE_DIRECTORY__ i
    File.WriteAllText(file, text)

let importInBulk name i chunk =
    let url = sprintf "http://localhost:9200/test2/%s/_bulk" name
    let line1 = """{ "create": {} }"""
    let content = chunk |> Seq.fold(fun s i -> sprintf "%s%s\n%s\n" s line1 i) ""
    //writeFile content i
    let sw = System.Diagnostics.Stopwatch.StartNew()
    Http.Request(url, httpMethod = "POST",
        headers = [ ContentType HttpContentTypes.Json ],
        body = TextRequest content) |> ignore
    sw.Stop()
    (chunk |> Seq.length |> float) / float sw.ElapsedMilliseconds

let importFromDbInBulk name select =

    let rec im i history coll =
        printfn "%i" i
        let newchunksize =
            match history with
            | h1::t1 ->
                match t1 with
                | h2::t2 ->

                | _ -> 2000
            | _ -> 1000

        let rate = coll |> Seq.take newchunksize |> Array.ofSeq |> importInBulk name i

        coll
        |> Seq.skip newchunksize
        |> im (i+1) ((rate, newchunksize)::history)

    DbSchema.GetDataContext()
    |> select
    |> Seq.map srlz
    |> im 0 []


importFromDbInBulk "CategoryChanges" (fun (db:Dbc) -> db.CategoryChanges)
