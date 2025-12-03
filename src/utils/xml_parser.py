import xml.etree.ElementTree as ET
from collections import defaultdict
import csv
import os
import json
import re
import glob
from utils import importer  as imp

# global variables
# --------------------------------------------------------
NS = "{http://www.basisdatensatz.de/oBDS/XML}"
TABLES = defaultdict(list)

# Global Counter (federated pk fpr op, sy, st, fe)
COUNTER = 1

COLUMN_ORDER_AICARE = {
    "patient": ["Patient_ID", "Register_ID_FK", "Geschlecht", "Geburtsdatum", "Verstorben", "Datum_Vitalstatus", "Todesursache_Grundleiden", "Todesursache_Grundleiden_Version", "Weitere_Todesursachen", "Weitere_Todesursachen_Version"],
    "tumor": ["Tumor_ID", "Register_ID_FK", "Patient_ID_FK", "Diagnosedatum", "Inzidenzort", "Diagnosesicherung", "Primaertumor_ICD", "Primaertumor_ICD_Version", "Primaertumor_Topographie_ICD_O", "Primaertumor_Topographie_ICD_O_Version", "Primaertumor_Morphologie_ICD_O", "Primaertumor_Morphologie_ICD_O_Version", "Primaertumor_LK_untersucht", "Primaertumor_LK_befallen", "Primaertumor_Grading", "cTNM_Version", "cTNM_y", "cTNM_r", "cTNM_a", "cTNM_praefix_T", "cTNM_T", "cTNM_praefix_N", "cTNM_N", "cTNM_praefix_M", "cTNM_M", "c_m_Symbol", "c_L.Kategorie", "c_V.Kategorie", "c_Pn.Kategorie", "c_S.Kategorie", "cTNM_UICC_Stadium", "Seitenlokalisation", "pTNM_Version", "pTNM_y", "pTNM_r", "pTNM_a", "pTNM_praefix_T", "pTNM_T", "pTNM_praefix_N", "pTNM_N", "pTNM_praefix_M", "pTNM_M", "p_m_Symbol", "p_L.Kategorie", "p_V.Kategorie", "p_Pn.Kategorie", "p_S.Kategorie", "pTNM_UICC_Stadium", "Primaertumor_DCN", "Anzahl_Tage_Diagnose_Tod", "Anzahl_Monate_Diagnose_Zensierung", "Primaerdiagnose_Menge_FM", "Weitere_Klassifikation_UICC", "Weitere_Klassifikation_Name", "Weitere_Klassifikation_Stadium", "Alter_bei_Diagnose"],
    "op": ["OP_ID", "Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Intention", "Datum_OP", "Anzahl_Tage_Diagnose_OP", "Beurteilung_Residualstatus", "Menge_OPS_code", "Menge_OPS_version"],
    "strahlentherapie": ["Bestrahlung_ID", "Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Intention_st", "Stellung_OP", "Beginn_Bestrahlung", "Anzahl_Tage_Diagnose_ST", "Anzahl_Tage_ST", "Applikationsart", "Applikationsspezifikation", "Zielgebiet_CodeVersion", "Zielgebiet_Code", "Seite_Zielgebiet"],
    "systemtherapie": ["SYST_ID", "Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Intention_sy", "Stellung_OP", "Beginn_SYST", "Anzahl_Tage_Diagnose_SYST", "Anzahl_Tage_SYST", "Therapieart", "Substanzen", "Protokolle"],
    "fe": ["Folgeereignis_ID", "Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Datum_Folgeereignis", "Folgeereignis_TNM_Version", "Folgeereignis_y_Symbol", "Folgeereignis_r_Symbol", "Folgeereignis_a_Symbol", "Folgeereignis_praefix_T", "Folgeereignis_TNM_T", "Folgeereignis_praefix_N", "Folgeereignis_TNM_N", "Folgeereignis_praefix_M", "Folgeereignis_TNM_M", "Folgeereignis_m_Symbol", "Folgeereignis_L_Kategorie", "Folgeereignis_V_Kategorie", "Folgeereignis_Pn_Kategorie", "Folgeereignis_S_Kategorie", "Folgeereignis_TNM_UICC", "Folgeereignis_Menge_weitere_Klassifikationen_Name", "Folgeereignis_Menge_weitere_Klassifikationen_Stadium", "Gesamtbeurteilung_Tumorstatus", "Verlauf_Lokaler_Tumorstatus", "Verlauf_Tumorstatus_Lymphknoten", "Verlauf_Tumorstatus_Fernmetastasen", "Menge_FM"]
}
# version: december 2025
COLUMN_USED_PIPELINE = {
    "patient": ["Patient_ID", "Register_ID_FK", "Geschlecht", "Geburtsdatum", "Verstorben", "Datum_Vitalstatus", "Todesursache_Grundleiden"],
    "tumor": ["Tumor_ID", "Register_ID_FK", "Patient_ID_FK", "Diagnosedatum", "Inzidenzort", "Diagnosesicherung", "Primaertumor_ICD", "Primaertumor_Topographie_ICD_O", "Primaertumor_Morphologie_ICD_O", "Primaertumor_LK_untersucht", "Primaertumor_LK_befallen", "Primaertumor_Grading", "cTNM_T", "cTNM_N", "cTNM_M", "cTNM_UICC_Stadium", "Seitenlokalisation", "pTNM_T", "pTNM_N", "pTNM_M", "pTNM_UICC_Stadium", "Anzahl_Tage_Diagnose_Tod", "Anzahl_Monate_Diagnose_Zensierung", "Primaerdiagnose_Menge_FM", "Alter_bei_Diagnose"],
    "op": ["Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Intention", "Datum_OP", "Anzahl_Tage_Diagnose_OP", "Beurteilung_Residualstatus", "Menge_OPS_code"],
    "strahlentherapie": ["Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Beginn_Bestrahlung", "Anzahl_Tage_Diagnose_ST", "Anzahl_Tage_ST", "Applikationsart", "Applikationsspezifikation"],
    "systemtherapie": ["Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Beginn_SYST", "Anzahl_Tage_Diagnose_SYST", "Anzahl_Tage_SYST", "Therapieart", "Substanzen"],
    "fe": ["Register_ID_FK", "Patient_ID_FK", "Tumor_ID_FK", "Datum_Folgeereignis", "Folgeereignis_TNM_T", "Folgeereignis_TNM_N", "Folgeereignis_TNM_M", "Gesamtbeurteilung_Tumorstatus", "Verlauf_Lokaler_Tumorstatus", "Verlauf_Tumorstatus_Lymphknoten", "Verlauf_Tumorstatus_Fernmetastasen", "Menge_FM"]
}


# helper functions
# --------------------------------------------------------
def add(table, row):
    TABLES[table].append(row)

def strip(tag):
    return re.sub(r"\{.*\}", "", tag)

def normalize_keys(data):
    """
    mapping dict to be interoperable with ai care
    """
    def load_mapping():
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_path = os.path.join(base_dir, "xml_mapper.json")

        if os.path.exists(mapping_path):
            with open(mapping_path, "r", encoding="utf-8") as f:
                return json.load(f)
        # else:
        #     print(f"Mapping file not found.")
        return {}
    
    mapping = load_mapping()

    cleaned = {}

    for k, v in data.items():
        if k in mapping:
            cleaned[mapping[k]] = v
        else:
            cleaned[k] = v

    return cleaned



# flatten funtions
# ---------------------------------------------------------
def flatten(elem, prefix=""):
    """Flatten XML text/attribute/child elememts."""
    data = {}
    tag = strip(elem.tag)
    full = f"{prefix}{tag}" if prefix else tag

    # flatten text
    if elem.text and elem.text.strip():
        data[full] = elem.text.strip()

    # flatten attribute
    for attr, val in elem.attrib.items():
        data[f"{full}_{attr}"] = val

    # flatten childs
    for child in elem:
        data.update(flatten(child, prefix=full + "_"))

    return data

def flatten_module(elem, pid, tid):
    """
    modules are extracted separately from Primaerdiagnose
    """
    tumor_data = {}
    modul_rows = []

    for child in elem:
        tag = strip(child.tag)
        if tag.startswith("Modul_"):
            # save seperately
            row = {"Patient_ID": pid, "Tumor_ID": tid}
            row.update(flatten(child))
            modul_rows.append((tag, row))
        else:
            # without tumor data
            data = {}
            # text and attribute of the child
            if child.text and child.text.strip():
                data[tag] = child.text.strip()
            for attr, val in child.attrib.items():
                data[f"{tag}_{attr}"] = val
            # childs extraction without modules
            for sub in child:
                sub_tag = strip(sub.tag)
                if not sub_tag.startswith("Modul_"):
                    data.update(flatten(sub, prefix=tag + "_"))
            tumor_data.update(data)

    return tumor_data, modul_rows


def flatten_skip_root(elem):
    """
    ignore Root-Tag.
    """
    data = {}
    for child in elem:
        data.update(flatten(child))
    return data


def flatten_applikationsart(rt):
    """
    extraction of application related data within a radiation in AI-CARE format.
    """

    data = {
        "Applikationsart": None,
        "Applikationsspezifikation": None,
        "Zielgebiet_CodeVersion": None,
        "Seite_Zielgebiet": None
    }

    appl = rt.find(NS + "Applikationsart")
    if appl is None:
        return data

    # find which application type: Perkutan, Metabolisch, Sonstige, Unbekannt
    for child in appl:
        typ = strip(child.tag)
        data["Applikationsart"] = typ

        specs = []  # specification text values for "Applikationsspezifikation" here

        # iterate over children inside the application type
        for sub in child:
            tag = strip(sub.tag)

            # aplication specification extraction
            if sub.text and sub.text.strip():
                # except target area + side, which are processed separately
                if tag not in ("Zielgebiet", "Seite_Zielgebiet"):
                    specs.append(sub.text.strip())

            # target area extraction
            if tag == "Zielgebiet":
                # CodeVersion2014 or 2021
                for z in sub:
                    if z.text and z.text.strip():
                        data["Zielgebiet_CodeVersion"] = z.text.strip()

            # target area side extraction 
            if tag == "Seite_Zielgebiet":
                if sub.text and sub.text.strip():
                    data["Seite_Zielgebiet"] = sub.text.strip()

        # aplication specification ; join specs
        if specs:
            data["Applikationsspezifikation"] = ";".join(specs)

    return data

# parser functions  (patient, tumor)
# --------------------------------------------------------
def parse_patient(patient, register):
    pid = patient.attrib.get("Patient_ID")

    # patient data without root tag
    base_data = patient.find(NS + "Patienten_Stammdaten")
    pdata = {"Patient_ID": pid, "Register_ID_FK": register}

    if base_data is not None:
        pdata.update(flatten_skip_root(base_data))  # ignore root-tag
    
    pdata = normalize_keys(pdata)

    add("patient", pdata)

    # add tumor data
    for tumor in patient.findall(".//" + NS + "Tumor"):
        parse_tumor(pid, register, tumor)


def parse_tumor(pid, rid, tumor):
    tid = tumor.attrib.get("Tumor_ID")
    tdata = {"Patient_ID_FK": pid, "Register_ID_FK" : rid, "Tumor_ID": tid}
    
    # Primaerdiagnose with modules
    prim = tumor.find(NS + "Primaerdiagnose")
    if prim is not None:

        tdata_flat, modules = flatten_module(prim, pid, tid)
        tdata.update(tdata_flat)

        # extract metastasis information ; delimited
        menge_fm_prim = prim.find(NS + "Menge_FM")
        pfm_list = []

        if menge_fm_prim is not None:
            for fm in menge_fm_prim.findall(NS + "Fernmetastase"):
                loc = fm.find(NS + "Lokalisation")
                if loc is not None and loc.text:
                    pfm_list.append(loc.text.strip())
        if pfm_list:
            tdata["Primaerdiagnose_Menge_FM"] = ";".join(pfm_list) if pfm_list else ""

        # ai care specifica
        tdata["Anzahl_Monate_Diagnose_Zensierung"] = ""
        monate_zens = prim.find(NS + "Anzahl_Monate_Diagnose_Zensierung")
        if monate_zens is not None and monate_zens.text:
            tdata["Anzahl_Monate_Diagnose_Zensierung"] = monate_zens.text.strip()

        # save module in separate tables
        for modul_name, row in modules:
            row["Register_ID_FK"] = rid # add register_id
            add(modul_name.replace("modul_", "Modul_").capitalize(), row)

    # add cTNM with prefix to tumor
    tnm = tumor.find(NS + "cTNM")
    if tnm is not None:
        tdata.update({f"TNM_{k}": v for k, v in flatten(tnm).items()})

    tdata = normalize_keys(tdata)
    add("tumor", tdata)
    
    # activate global counter for federated fk
    global COUNTER

    # OP
    for op in tumor.findall(".//" + NS + "Menge_OP/" + NS + "OP"):
        row = {"OP_ID": COUNTER, "Patient_ID_FK": pid, "Register_ID_FK": rid, "Tumor_ID_FK": tid}
        COUNTER += 1  

        # extract op data
        for child in op:
            tag = strip(child.tag)
            # Menge_OPS is treated separately
            if tag != "Menge_OPS":
                if child.text and child.text.strip():
                    row[tag] = child.text.strip()
                for attr, val in child.attrib.items():
                    row[f"{tag}_{attr}"] = val

        # extract ops codes + version ; delimited
        ops_codes = []
        ops_versions = []
        menge_ops = op.find(NS + "Menge_OPS")
        if menge_ops is not None:
            for ops in menge_ops.findall(NS + "OPS"):
                code = ops.find(NS + "Code")
                version = ops.find(NS + "Version")
                if code is not None and code.text:
                    ops_codes.append(code.text.strip())
                if version is not None and version.text:
                    ops_versions.append(version.text.strip())

        if ops_codes:
            row["Menge_OPS_code"] = ";".join(ops_codes)
        if ops_versions:
            row["Menge_OPS_version"] = ";".join(ops_versions)

        # column renaming
        row = normalize_keys(row)
        add("op", row)

    # systemtherapie
    for syst in tumor.findall(".//" + NS + "Menge_SYST/" + NS + "SYST"):
        row = {"SYST_ID": COUNTER, "Patient_ID_FK": pid, "Register_ID_FK" : rid, "Tumor_ID_FK": tid}
        COUNTER += 1

        for child in syst:
            tag = strip(child.tag)
            if tag != "Menge_Substanz":
                if child.text and child.text.strip():
                    row[tag] = child.text.strip()
                for attr, val in child.attrib.items():
                    row[f"{tag}_{attr}"] = val

        # extract protokoll
        for prot in syst.findall(NS + "Protokoll"):
            row.update(flatten(prot))  # with prefix
        
        # extract atc codes/version + substanzen ; delimited    
        subst_tokens = []
        menge_subst = syst.find(NS + "Menge_Substanz")

        if menge_subst is not None:
            for subst in menge_subst.findall(NS + "Substanz"):

                # extract atc with tag for special ai-care formatting
                atc = subst.find(NS + "ATC")
                if atc is not None:
                    code = atc.find(NS + "Code")
                    version = atc.find(NS + "Version")

                    if code is not None and code.text:
                        combined = code.text.strip()
                        if version is not None and version.text:
                            combined += f" {version.text.strip()}"

                        subst_tokens.append(("ATC", combined)) # tag for ai care formatting

                # extract non-atc substances with tag (!!!) for special formatting
                bezeichnung = subst.find(NS + "Bezeichnung")
                if bezeichnung is not None and bezeichnung.text:
                    subst_tokens.append(("BEZ", bezeichnung.text.strip())) # tag for ai care formatting


        # special ai care delimiter formatting
        if subst_tokens:
            if len(subst_tokens) == 1:
                row["Substanzen"] = subst_tokens[0][1]
            else:
                result = []

                for i, (typ, val) in enumerate(subst_tokens):
                    result.append(val)
                    if i < len(subst_tokens) - 1:
                        next_typ = subst_tokens[i+1][0]

                        # RULES, RULES, RULES
                        # ATC -> ATC : " ; "
                        if typ == "ATC" and next_typ == "ATC":
                            result.append(" ; ")
                        # ATC -> BEZ : " ;"
                        elif typ == "ATC" and next_typ == "BEZ":
                            result.append(" ;")
                        # BEZ -> ATC : "; "
                        elif typ == "BEZ" and next_typ == "ATC":
                            result.append("; ")
                        # BEZ -> BEZ : ";"
                        elif typ == "BEZ" and next_typ == "BEZ":
                            result.append(";")
                # bring it together
                row["Substanzen"] = "".join(result)
            
        # renaming without xml_mapper.json
        if "Intention" in row:
            row["Intention_sy"] = row.pop("Intention")

        row = normalize_keys(row)
        add("systemtherapie", row)

    # strahlentherapie
    # extract intention and stellung_op 
    for st in tumor.findall(".//" + NS + "Menge_ST/" + NS + "ST"):
        intention = st.find(NS + "Intention")
        stellung_op = st.find(NS + "Stellung_OP")

        intention_val = intention.text.strip() if intention is not None and intention.text else None
        stellung_op_val = stellung_op.text.strip() if stellung_op is not None and stellung_op.text else None

        # extract all radiation information under ST
        for rt in st.findall(".//" + NS + "Menge_Bestrahlung/" + NS + "Bestrahlung"):

            row = {
                "Bestrahlung_ID": COUNTER,
                "Patient_ID_FK": pid,
                "Register_ID_FK": rid,
                "Tumor_ID_FK": tid,
                "Intention_st": intention_val,
                "Stellung_OP": stellung_op_val
            }
            COUNTER += 1

            # extract bestrahlung
            row.update(flatten_skip_root(rt)) # without prefic

            # extract applikationsart
            row.update(flatten_applikationsart(rt))
            # remove duplicated field from flatten_skip_root and include only valid application data 
            REMOVE_RT_FIELDS = [
                    "Applikationsart_Perkutan_Atemgetriggert",
                    "Applikationsart_Perkutan_Radiochemo",
                    "Applikationsart_Perkutan_Seite_Zielgebiet",
                    "Applikationsart_Perkutan_Stereotaktisch",
                    "Applikationsart_Perkutan_Zielgebiet_CodeVersion2014",
                    "Applikationsart_Perkutan_Zielgebiet_CodeVersion2021",
                    "Applikationsart_Metabolisch_Metabolisch_Typ",
                    "Applikationsart_Metabolisch_Seite_Zielgebiet",
                    "Applikationsart_Metabolisch_Zielgebiet_CodeVersion2014",
                    "Applikationsart_Metabolisch_Zielgebiet_CodeVersion2021",
                    "Applikationsart_Sonstige_Seite_Zielgebiet",
                    "Applikationsart_Sonstige_Zielgebiet_CodeVersion2021",
                    "Applikationsart_Unbekannt_Seite_Zielgebiet",
                    "Applikationsart_Unbekannt_Zielgebiet_CodeVersion2014",
                    "Applikationsart_Unbekannt_Zielgebiet_CodeVersion2021",
                    ]
            for key in list(row.keys()):
                if key in REMOVE_RT_FIELDS:
                    del row[key]

            row = normalize_keys(row)
            add("strahlentherapie", row)

    # course
    for verlauf in tumor.findall(".//" + NS + "Menge_Folgeereignis/" + NS + "Folgeereignis"):
        row = {"FE_ID": COUNTER ,"Patient_ID_FK": pid, "Register_ID_FK" : rid, "Tumor_ID_FK": tid}
        COUNTER += 1
        row.update(flatten_skip_root(verlauf))

        # menge_fm from folgeereignis for metastases extraction
        fm_liste = []
        menge_fm_elem = verlauf.find(NS + "Menge_FM")
        if menge_fm_elem is not None:
            for fm in menge_fm_elem.findall(NS + "Fernmetastase"):
                loc = fm.find(NS + "Lokalisation")
                if loc is not None and loc.text:
                    fm_liste.append(loc.text.strip())

        if fm_liste:
            row["Menge_FM"] = ";".join(fm_liste) if fm_liste else "" # read meatastases localisation ; separated
        else:
            row["Menge_FM"] = ""
        
        row = normalize_keys(row)
        add("fe", row)



# save tables to csv
# --------------------------------------------------------
def write_tables(outdir="src/data"):
    os.makedirs(outdir, exist_ok=True)

    for table, rows in TABLES.items():
        if not rows:
            continue
        
        all_fields = sorted({k for r in rows for k in r})
        if table in COLUMN_ORDER_AICARE:
            base_fields = COLUMN_ORDER_AICARE[table]
            
            # fields used in pipeline, but not in input xml
            # missing_fields = [f for f in COLUMN_USED_PIPELINE[table] if f not in all_fields]
            # if missing_fields:
            #     print(f"Missing columns used in pipeline:'{table}': {missing_fields}")
            
            # fields not in ai_care, but in input xml
            extra_fields = [f for f in all_fields if f not in base_fields]
            # if extra_fields:
            #     print(f"Columns not used in pipeline'{table}': {extra_fields}")
            fields = base_fields + extra_fields
        else:
            fields = sorted({k for r in rows for k in r})

        with open(os.path.join(outdir, f"{table}.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter=",")
            w.writeheader()
            w.writerows(rows)
        # print(f"{table}.csv generated.")



# main function
# --------------------------------------------------------
def parse_file(xml_filename):
    tree = ET.parse(xml_filename)
    root = tree.getroot()

    register = root.find(NS + "Lieferregister")
    register_id = register.attrib.get("Register_ID") if register is not None else None

    for patient in root.findall(NS + "Menge_Patient/" + NS + "Patient"):
        parse_patient(patient, register_id)

    write_tables()


if __name__ == "__main__":
    parse_file(glob.glob(os.path.join(imp.load_config('iam_path_zfkd_data'), "*.xml"))[0])
