// SPDX-FileCopyrightText: 2026 Enversion Journl ApS
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of Healthpipe. See the LICENSE file in the project
// root for the full AGPL-3.0 license text.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Xml;

/// <summary>
/// Standalone C# XML parser for AKTIVITETSRESULTAT, ported from the original
/// SSIS ScriptMain in Extract_UDPAKAKTIVITETXML.dtsx.
/// Reads XML strings from stdin (JSON array), parses them, and outputs
/// results as JSON for comparison with the Python implementation.
/// </summary>
class Program
{
    static void Main(string[] args)
    {
        bool benchmarkMode = false;
        int benchmarkLoops = 1;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--benchmark")
            {
                benchmarkMode = true;
            }
            if (args[i] == "--loops" && i + 1 < args.Length && int.TryParse(args[i + 1], out int loops))
            {
                benchmarkLoops = Math.Max(1, loops);
            }
        }

        string input = Console.In.ReadToEnd();
        var testCases = JsonSerializer.Deserialize<List<TestInput>>(input);

        if (benchmarkMode)
        {
            var sw = Stopwatch.StartNew();
            long totalRows = 0;
            long totalCases = 0;

            for (int loop = 0; loop < benchmarkLoops; loop++)
            {
                foreach (var tc in testCases)
                {
                    var parser = new AktivitetsresultatParser();
                    var output = parser.Parse(tc.xml, tc.aktivitets_id);
                    totalRows += output.element_count;
                    totalCases++;
                }
            }
            sw.Stop();

            var benchmarkOut = new BenchmarkOutput
            {
                elapsed_ms = sw.Elapsed.TotalMilliseconds,
                total_rows = totalRows,
                total_cases = totalCases,
                loops = benchmarkLoops
            };
            Console.Write(JsonSerializer.Serialize(benchmarkOut));
            return;
        }
        else
        {
            var allResults = new List<TestOutput>();
            foreach (var tc in testCases)
            {
                var parser = new AktivitetsresultatParser();
                var output = parser.Parse(tc.xml, tc.aktivitets_id);
                output.test_id = tc.test_id;
                allResults.Add(output);
            }

            var options = new JsonSerializerOptions { WriteIndented = true };
            Console.Write(JsonSerializer.Serialize(allResults, options));
        }
    }
}

class TestInput
{
    public string test_id { get; set; }
    public string xml { get; set; }
    public int aktivitets_id { get; set; }
}

class TestOutput
{
    public string test_id { get; set; }
    public int aktivitets_id { get; set; }
    public int element_count { get; set; }
    public List<ResultRow> results { get; set; } = new();
    public KontekstRow kontekst { get; set; }
    public string error { get; set; }
}

class BenchmarkOutput
{
    public double elapsed_ms { get; set; }
    public long total_rows { get; set; }
    public long total_cases { get; set; }
    public int loops { get; set; }
}

class KontekstRow
{
    public int AktivitetsId { get; set; }
    public string OprettetBruger { get; set; }
    public string OprettetPersId { get; set; }
    public string OprettetRolle { get; set; }
    public string OprettetAdmEnhed { get; set; }
    public string OprettetAdmEnhedId { get; set; }
    public string OprettetTidspunkt { get; set; }
    public int OprettetTidsstempel { get; set; }
    public string AendretBruger { get; set; }
    public string AendretPersId { get; set; }
    public string AendretRolle { get; set; }
    public string AendretAdmEnhed { get; set; }
    public string AendretAdmEnhedId { get; set; }
    public string AendretTidspunkt { get; set; }
    public int AendretTidsstempel { get; set; }
    public string BesluttetBruger { get; set; }
    public string BesluttetPersId { get; set; }
    public string BesluttetRolle { get; set; }
    public string BesluttetAdmEnhed { get; set; }
    public string BesluttetAdmEnhedId { get; set; }
    public string BesluttetTidspunkt { get; set; }
    public int BesluttetTidsstempel { get; set; }
    public string Status { get; set; }
}

class ResultRow
{
    public int? ElementNummer { get; set; }
    public int? KlyngeId { get; set; }
    public string KlyngeIdHierarki { get; set; }
    public int KlyngeNiveau { get; set; }
    public string KlyngeNavne { get; set; }
    public string KlyngeLedetekster { get; set; }
    public int? KlyngeParentId { get; set; }
    public int? RepetitionId { get; set; }
    public string RepetitionIdHierarki { get; set; }
    public int RepetitionNiveau { get; set; }
    public string Hierarki { get; set; }
    public string Overskrift { get; set; }
    public string Ledetekst { get; set; }
    public string Type { get; set; }
    public string Kladde { get; set; }
    public string KlyngeNavn { get; set; }
    public string KlyngeLedetekst { get; set; }
    public string KlyngeValgLedetekst { get; set; }
    public string KlyngeType { get; set; }
    public string ElementNavn { get; set; }
    public string ElementLedetekst { get; set; }
    public string ElementVaerdi { get; set; }
    public string ElementElementId { get; set; }
    public string ElementKode { get; set; }
    public string ElementOriginalKode { get; set; }
    public string ElementAdministrativEnhedId { get; set; }
    public string ElementPersId { get; set; }
    public string ElementType { get; set; }
    public string ElementMoid { get; set; }
    public string RaekkeType { get; set; }
    public int XmlStatus { get; set; }
    public string XmlStatusText { get; set; }
}

class AktivitetsresultatParser
{
    private List<ResultRow> aktivitetsresultatListe = new();
    private KontekstRow kontekst;

    // Counters matching C# SSIS ScriptMain
    private int repCounter = 0;
    private int klyCounter = 0;
    private int elemCounter = 0;

    // Hierarchy tracking
    private List<int> klyngeIdHierarki = new();
    private List<int> repetitionIdHierarki = new();
    private List<string> hierarki = new();
    private List<string> klyngeNavne = new();
    private List<string> klyngeLedetekster = new();
    private List<string> klyngeTyper = new();

    // State
    private string overskrift;
    private string ledetekst;
    private string type;
    private string kladde;
    private string personaleRolleLedetekst;

    public TestOutput Parse(string xmlString, int aktivitetsId)
    {
        var output = new TestOutput { aktivitets_id = aktivitetsId };

        kontekst = new KontekstRow { AktivitetsId = aktivitetsId };

        if (string.IsNullOrWhiteSpace(xmlString))
        {
            output.error = "XML Fejl";
            output.results.Add(new ResultRow
            {
                RaekkeType = null,
                XmlStatus = -1,
                XmlStatusText = "XML Fejl"
            });
            output.kontekst = kontekst;
            output.element_count = output.results.Count;
            return output;
        }

        XmlDocument xDoc;
        try
        {
            xDoc = new XmlDocument();
            xDoc.LoadXml(xmlString);
        }
        catch
        {
            output.error = "XML Fejl";
            output.results.Add(new ResultRow
            {
                RaekkeType = null,
                XmlStatus = -1,
                XmlStatusText = "XML Fejl"
            });
            output.kontekst = kontekst;
            output.element_count = output.results.Count;
            return output;
        }

        // Process root node (Afdelingsydelsesresultat)
        XmlNode root = xDoc.DocumentElement;
        handleAfdelingsydelsesresultat(root);

        // Process Kontekst
        XmlNode ctxNode = root["Kontekst"];
        if (ctxNode != null)
        {
            handleKontekst(ctxNode);
        }

        // Traverse children
        foreach (XmlNode child in root.ChildNodes)
        {
            // Reset per top-level child
            klyngeIdHierarki = new List<int>();
            repetitionIdHierarki = new List<int>();
            hierarki = new List<string>();
            klyngeNavne = new List<string>();
            klyngeLedetekster = new List<string>();
            klyngeTyper = new List<string>();
            personaleRolleLedetekst = null;

            handleGruppering(child, 0, 0);
        }

        if (aktivitetsresultatListe.Count == 0)
        {
            aktivitetsresultatListe.Add(new ResultRow
            {
                Overskrift = trimReplace(overskrift),
                Ledetekst = trimReplace(ledetekst),
                Type = trimReplace(type),
                Kladde = trimReplace(kladde),
                ElementVaerdi = "",
                RaekkeType = "Tom",
                XmlStatus = 0,
                XmlStatusText = "Ingen Elementer"
            });
        }

        output.results = aktivitetsresultatListe;
        output.kontekst = kontekst;
        output.element_count = aktivitetsresultatListe.Count;
        return output;
    }

    private void handleAfdelingsydelsesresultat(XmlNode node)
    {
        if (node["Overskrift"] != null) overskrift = node["Overskrift"].InnerText;
        if (node["Ledetekst"] != null) ledetekst = node["Ledetekst"].InnerText;
        if (node["Type"] != null) type = node["Type"].InnerText;
        if (node["Kladde"] != null) kladde = node["Kladde"].InnerText;
    }

    private void handleKontekst(XmlNode ctxnode)
    {
        foreach (XmlNode node in ctxnode.ChildNodes)
        {
            // Oprettet
            if (node.Name == "Oprettet")
            {
                if (node["Bruger"] != null) kontekst.OprettetBruger = node["Bruger"].InnerText;
                if (node["PersId"] != null) kontekst.OprettetPersId = node["PersId"].InnerText;
                if (node["Rolle"] != null) kontekst.OprettetRolle = node["Rolle"].InnerText;
                if (node["AdministrativEnhed"] != null) kontekst.OprettetAdmEnhed = node["AdministrativEnhed"].InnerText;
                if (node["AdministrativEnhedId"] != null) kontekst.OprettetAdmEnhedId = node["AdministrativEnhedId"].InnerText;
                if (node["Tidspunkt"] != null) kontekst.OprettetTidspunkt = node["Tidspunkt"].InnerText;
                if (node["Tidsstempel"] != null)
                {
                    int.TryParse(node["Tidsstempel"].InnerText, out int ts);
                    kontekst.OprettetTidsstempel = ts;
                }
            }
            // Aendret
            if (node.Name == "Aendret")
            {
                if (node["Bruger"] != null) kontekst.AendretBruger = node["Bruger"].InnerText;
                if (node["PersId"] != null) kontekst.AendretPersId = node["PersId"].InnerText;
                if (node["Rolle"] != null) kontekst.AendretRolle = node["Rolle"].InnerText;
                if (node["AdministrativEnhed"] != null) kontekst.AendretAdmEnhed = node["AdministrativEnhed"].InnerText;
                if (node["AdministrativEnhedId"] != null) kontekst.AendretAdmEnhedId = node["AdministrativEnhedId"].InnerText;
                if (node["Tidspunkt"] != null) kontekst.AendretTidspunkt = node["Tidspunkt"].InnerText;
                if (node["Tidsstempel"] != null)
                {
                    int.TryParse(node["Tidsstempel"].InnerText, out int ts);
                    kontekst.AendretTidsstempel = ts;
                }
            }
            // Besluttet
            if (node.Name == "Besluttet")
            {
                if (node["Bruger"] != null) kontekst.BesluttetBruger = node["Bruger"].InnerText;
                if (node["PersId"] != null) kontekst.BesluttetPersId = node["PersId"].InnerText;
                if (node["Rolle"] != null) kontekst.BesluttetRolle = node["Rolle"].InnerText;
                if (node["AdministrativEnhed"] != null) kontekst.BesluttetAdmEnhed = node["AdministrativEnhed"].InnerText;
                if (node["AdministrativEnhedId"] != null) kontekst.BesluttetAdmEnhedId = node["AdministrativEnhedId"].InnerText;
                if (node["Tidspunkt"] != null) kontekst.BesluttetTidspunkt = node["Tidspunkt"].InnerText;
                if (node["Tidsstempel"] != null)
                {
                    int.TryParse(node["Tidsstempel"].InnerText, out int ts);
                    kontekst.BesluttetTidsstempel = ts;
                }
            }
            // Status
            if (node["Status"] != null)
            {
                kontekst.Status = node["Status"].InnerText;
            }
        }
    }

    private void handleGruppering(XmlNode node, int repDepth, int klyDepth)
    {
        if (node.Name == "Element")
        {
            handleElement(node, repDepth, klyDepth);
        }
        else if (node.Name == "Klynge" && node.Attributes["type"]?.Value == "selektion")
        {
            klyCounter++;
            klyDepth++;
            klyngeIdHierarki.Add(klyCounter);
            hierarki.Add("Kly" + klyCounter);

            string klyName = node["Navn"]?.InnerText ?? "";
            string klyLedetekst = node["Ledetekst"]?.InnerText ?? "";
            string klyType = node.Attributes["type"]?.Value ?? "";
            klyngeNavne.Add(klyName);
            klyngeLedetekster.Add(klyLedetekst);
            klyngeTyper.Add(klyType);

            elemCounter++;
            handleSelektion(node, repDepth, klyDepth);

            foreach (XmlNode child in node.ChildNodes)
            {
                handleGruppering(child, repDepth, klyDepth);
            }

            klyDepth--;
            klyngeIdHierarki.RemoveAt(klyngeIdHierarki.Count - 1);
            hierarki.RemoveAt(hierarki.Count - 1);
            klyngeNavne.RemoveAt(klyngeNavne.Count - 1);
            klyngeLedetekster.RemoveAt(klyngeLedetekster.Count - 1);
            klyngeTyper.RemoveAt(klyngeTyper.Count - 1);
        }
        else if (node.Name == "Repetition")
        {
            repCounter++;
            repDepth++;
            repetitionIdHierarki.Add(repCounter);
            hierarki.Add("Rep" + repCounter);

            foreach (XmlNode child in node.ChildNodes)
            {
                handleGruppering(child, repDepth, klyDepth);
            }

            repDepth--;
            repetitionIdHierarki.RemoveAt(repetitionIdHierarki.Count - 1);
            hierarki.RemoveAt(hierarki.Count - 1);
        }
        else if (node.Name == "Klynge")
        {
            klyCounter++;
            klyDepth++;
            klyngeIdHierarki.Add(klyCounter);
            hierarki.Add("Kly" + klyCounter);

            string klyName = node["Navn"]?.InnerText ?? "";
            string klyLedetekst = node["Ledetekst"]?.InnerText ?? "";
            string klyType = node.Attributes["type"]?.Value ?? "";
            klyngeNavne.Add(klyName);
            klyngeLedetekster.Add(klyLedetekst);
            klyngeTyper.Add(klyType);

            foreach (XmlNode child in node.ChildNodes)
            {
                handleGruppering(child, repDepth, klyDepth);
            }

            klyDepth--;
            klyngeIdHierarki.RemoveAt(klyngeIdHierarki.Count - 1);
            hierarki.RemoveAt(hierarki.Count - 1);
            klyngeNavne.RemoveAt(klyngeNavne.Count - 1);
            klyngeLedetekster.RemoveAt(klyngeLedetekster.Count - 1);
            klyngeTyper.RemoveAt(klyngeTyper.Count - 1);
        }
    }

    private void handleElement(XmlNode node, int repDepth, int klyDepth)
    {
        string name = node["Navn"]?.InnerText;

        if (name == "PersonaleRolle" && personaleRolleLedetekst == null)
        {
            personaleRolleLedetekst = node["Vaerdi"]?.InnerText;
            return;
        }

        elemCounter++;

        int? klyngeId = null;
        string klyngeNavn = null;
        string klyngeLedetekstVal = null;
        string klyngeType = null;

        if (klyDepth > 0 && klyngeIdHierarki.Count > 0)
        {
            int idx = klyngeIdHierarki.Count - 1;
            klyngeId = klyngeIdHierarki[idx];
            klyngeNavn = idx < klyngeNavne.Count ? klyngeNavne[idx] : null;
            klyngeLedetekstVal = idx < klyngeLedetekster.Count ? klyngeLedetekster[idx] : null;
            klyngeType = idx < klyngeTyper.Count ? klyngeTyper[idx] : null;
        }

        int? klyngeParentId = null;
        if (klyngeIdHierarki.Count > 1 && klyDepth > 1)
        {
            klyngeParentId = klyngeIdHierarki[klyngeIdHierarki.Count - 2];
        }

        string elementLedetekst = null;
        if (personaleRolleLedetekst != null && name == "Personale")
        {
            elementLedetekst = personaleRolleLedetekst;
        }
        else
        {
            elementLedetekst = node["Ledetekst"]?.InnerText;
        }
        personaleRolleLedetekst = null;

        var row = new ResultRow
        {
            ElementNummer = elemCounter,
            KlyngeId = klyngeId,
            KlyngeIdHierarki = klyngeIdHierarki.Count > 0 ? string.Join("#", klyngeIdHierarki) : null,
            KlyngeNiveau = klyDepth,
            KlyngeNavne = klyngeNavne.Count > 0 ? string.Join("#", klyngeNavne) : null,
            KlyngeLedetekster = klyngeLedetekster.Count > 0 ? string.Join("#", klyngeLedetekster) : null,
            KlyngeParentId = klyngeParentId,
            RepetitionId = repDepth > 0 ? repCounter : null,
            RepetitionIdHierarki = repetitionIdHierarki.Count > 0 ? string.Join("#", repetitionIdHierarki) : null,
            RepetitionNiveau = repDepth,
            Hierarki = hierarki.Count > 0 ? string.Join("#", hierarki) : null,
            Overskrift = trimReplace(overskrift),
            Ledetekst = trimReplace(ledetekst),
            Type = trimReplace(type),
            Kladde = trimReplace(kladde),
            KlyngeNavn = trimReplace(klyngeNavn),
            KlyngeLedetekst = trimReplace(klyngeLedetekstVal),
            KlyngeValgLedetekst = null,
            KlyngeType = trimReplace(klyngeType),
            ElementNavn = trimReplace(name),
            ElementLedetekst = trimReplace(elementLedetekst),
            ElementVaerdi = node["Vaerdi"]?.InnerText ?? "",
            ElementElementId = trimReplace(node["ElementId"]?.InnerText),
            ElementKode = trimReplace(node["Kode"]?.InnerText),
            ElementOriginalKode = trimReplace(node["Originalkode"]?.InnerText),
            ElementAdministrativEnhedId = trimReplace(node["AdministrativEnhedId"]?.InnerText),
            ElementPersId = trimReplace(node["PersId"]?.InnerText),
            ElementType = trimReplace(node.Attributes["type"]?.Value),
            ElementMoid = trimReplace(node["MOID"]?.InnerText),
            RaekkeType = "Element",
            XmlStatus = 1,
            XmlStatusText = "God XML"
        };

        aktivitetsresultatListe.Add(row);
    }

    private void handleSelektion(XmlNode node, int repDepth, int klyDepth)
    {
        int? klyngeParentId = null;
        if (klyngeIdHierarki.Count > 1 && klyDepth > 1)
        {
            klyngeParentId = klyngeIdHierarki[klyngeIdHierarki.Count - 2];
        }

        var row = new ResultRow
        {
            ElementNummer = elemCounter,
            KlyngeId = klyDepth > 0 ? klyCounter : null,
            KlyngeIdHierarki = klyngeIdHierarki.Count > 0 ? string.Join("#", klyngeIdHierarki) : null,
            KlyngeNiveau = klyDepth,
            KlyngeNavne = klyngeNavne.Count > 0 ? string.Join("#", klyngeNavne) : null,
            KlyngeLedetekster = klyngeLedetekster.Count > 0 ? string.Join("#", klyngeLedetekster) : null,
            KlyngeParentId = klyngeParentId,
            RepetitionId = repDepth > 0 ? repCounter : null,
            RepetitionIdHierarki = repetitionIdHierarki.Count > 0 ? string.Join("#", repetitionIdHierarki) : null,
            RepetitionNiveau = repDepth,
            Hierarki = hierarki.Count > 0 ? string.Join("#", hierarki) : null,
            Overskrift = trimReplace(overskrift),
            Ledetekst = trimReplace(ledetekst),
            Type = trimReplace(type),
            Kladde = trimReplace(kladde),
            KlyngeNavn = trimReplace(node["Navn"]?.InnerText),
            KlyngeLedetekst = trimReplace(node["Ledetekst"]?.InnerText),
            KlyngeValgLedetekst = trimReplace(node["ValgLedetekst"]?.InnerText),
            KlyngeType = trimReplace(node.Attributes["type"]?.Value),
            ElementNavn = null,
            ElementLedetekst = null,
            ElementVaerdi = "",
            ElementElementId = null,
            ElementKode = null,
            ElementOriginalKode = null,
            ElementAdministrativEnhedId = null,
            ElementPersId = null,
            ElementType = null,
            ElementMoid = null,
            RaekkeType = "Selektion",
            XmlStatus = 1,
            XmlStatusText = "God XML"
        };

        aktivitetsresultatListe.Add(row);
    }

    private string trimReplace(string val)
    {
        if (val == null) return null;
        val = val.Replace("\n", "").Replace("\r", "").Trim();
        return string.IsNullOrEmpty(val) ? null : val;
    }
}
