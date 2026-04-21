# ══════════════════════════════════════════════════════════════════════════════
#  SEFAZ – Endpoint de Consulta de Situação da NF-e / NFC-e / CT-e
#  Adicione este arquivo ao seu projeto FastAPI e inclua o router no main.py
# ══════════════════════════════════════════════════════════════════════════════
#
#  Instalação das dependências:
#      pip install httpx lxml
#
#  No seu main.py (ou onde você registra as rotas):
#      from sefaz_consulta import router as sefaz_router
#      app.include_router(sefaz_router)
#
# ══════════════════════════════════════════════════════════════════════════════

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
from lxml import etree

router = APIRouter()


# ── Modelos ────────────────────────────────────────────────────────────────────

class SefazConsultaRequest(BaseModel):
    chave: str  # chave de acesso de 44 dígitos


class SefazConsultaResponse(BaseModel):
    cStat: str
    xMotivo: str
    tpAmb: str   # "1" = produção, "2" = homologação
    modelo: str  # "55" = NF-e, "65" = NFC-e, "57" = CT-e


# ── Helpers ────────────────────────────────────────────────────────────────────

# Mapa de cUF → URL do webservice SEFAZ (NFeConsultaProtocolo4)
# UFs sem servidor próprio delegam para SVRS (Sefaz Virtual Rio Grande do Sul)
# ou SVCAN (Sefaz Virtual do Ambiente Nacional) para CT-e.
_UF_URLS_NFE_PROD = {
    "11": "https://nfe.sefazro.gov.br/nfe/services/NFeConsulta4",          # RO
    "12": "https://issnfe.sefaznet.ac.gov.br/nfe/services/NFeConsulta4",   # AC → SVRS
    "13": "https://nfe.sefaz.am.gov.br/services/services/NfeConsulta4",    # AM → SVRS
    "15": "https://nfe.sefa.pa.gov.br/nfe/services/NFeConsulta4",          # PA → SVRS
    "21": "https://nfe.sefaz.ma.gov.br/nfe/services/NFeConsulta4",         # MA → SVRS
    "22": "https://nfe.sefaz.pi.gov.br/nfe/services/NFeConsulta4",         # PI → SVRS
    "23": "https://nfe.sefaz.ce.gov.br/nfe/services/NFeConsulta4",         # CE → SVRS
    "24": "https://nfe.set.rn.gov.br/nfe/services/NFeConsulta4",           # RN → SVRS
    "25": "https://nfe.set.pb.gov.br/nfe/services/NFeConsulta4",           # PB → SVRS
    "26": "https://nfe.sefaz.pe.gov.br/nfe/services/NFeConsulta4",         # PE
    "27": "https://nfe.sefaz.al.gov.br/nfe/services/NFeConsulta4",         # AL → SVRS
    "28": "https://nfe.se.gov.br/nfe/services/NFeConsulta4",               # SE → SVRS
    "29": "https://nfe.sefaz.ba.gov.br/webservices/NFeConsulta4/NFeConsulta4", # BA
    "31": "https://nfe.fazenda.mg.gov.br/nfe/services/NFeConsulta4",       # MG
    "32": "https://nfe.sefaz.es.gov.br/nfe/services/NFeConsulta4",         # ES → SVRS
    "33": "https://nfe.fazenda.rj.gov.br/nfe/services/NFeConsulta4",       # RJ → SVRS
    "35": "https://nfe.fazenda.sp.gov.br/ws/nfeconsultaprotocolo4.asmx",  # SP
    "41": "https://nfe.sefa.pr.gov.br/nfe/services/NFeConsulta4",          # PR
    "42": "https://nfe.sef.sc.gov.br/nfe/services/NFeConsulta4",           # SC → SVRS
    "43": "https://nfe.sefazrs.rs.gov.br/ws/NfeConsulta/NfeConsulta4.asmx", # RS
    "50": "https://nfe.sefaz.ms.gov.br/nfe/services/NFeConsulta4",         # MS → SVRS
    "51": "https://nfe.sefaz.mt.gov.br/nfe/services/v4/NFeConsulta4",      # MT
    "52": "https://nfe.sefaz.go.gov.br/nfe/services/NFeConsulta4",         # GO → SVRS
    "53": "https://nfe.fazenda.df.gov.br/nfe/services/NFeConsulta4",       # DF → SVRS
}

# SVRS é o fallback para UFs sem servidor próprio
_SVRS_NFE_PROD = "https://nfe.svrs.fazenda.gov.br/ws/NfeConsulta/NfeConsulta4.asmx"
_SVRS_NFE_HOM  = "https://hom.nfe.fazenda.gov.br/ws/NfeConsulta/NfeConsulta4.asmx"

# CT-e usa SVCAN
_SVCAN_CTE_PROD = "https://cte.svrs.fazenda.gov.br/ws/cteconsulta4/cteconsulta4.asmx"
_SVCAN_CTE_HOM  = "https://hnfe.sefa.pa.gov.br/cte-ws/services/CteconsultaV4"

SOAP_ACTION_NFE = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeConsultaProtocolo4/nfeConsultaNF"
SOAP_ACTION_CTE = "http://www.portalfiscal.inf.br/cte/wsdl/CTeConsultaProtocolo4/cteConsultaCT"

WSDL_NFE = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeConsultaProtocolo4"
WSDL_CTE = "http://www.portalfiscal.inf.br/cte/wsdl/CTeConsultaProtocolo4"


def _get_endpoint(chave44: str) -> tuple[str, str, str]:
    """
    Retorna (url, soap_action, wsdl_ns) para a chave fornecida.
    """
    c_uf   = chave44[:2]
    tp_amb = chave44[20]   # '1' = produção, '2' = homologação
    mod    = chave44[20:22]  # posição 20-21: modelo do documento

    # CT-e (modelo 57)
    if mod == "57":
        url = _SVCAN_CTE_HOM if tp_amb == "2" else _SVCAN_CTE_PROD
        return url, SOAP_ACTION_CTE, WSDL_CTE

    # NFC-e (modelo 65) → SVRS
    if mod == "65":
        url = _SVRS_NFE_HOM if tp_amb == "2" else _SVRS_NFE_PROD
        return url, SOAP_ACTION_NFE, WSDL_NFE

    # NF-e (modelo 55)
    if tp_amb == "2":
        url = _SVRS_NFE_HOM
    else:
        url = _UF_URLS_NFE_PROD.get(c_uf, _SVRS_NFE_PROD)

    return url, SOAP_ACTION_NFE, WSDL_NFE


def _build_soap_nfe(chave44: str, tp_amb: str, c_uf: str, wsdl_ns: str) -> bytes:
    """Monta o envelope SOAP/12 para NFeConsultaProtocolo4 ou CTeConsultaProtocolo4."""
    modelo = chave44[20:22]
    is_cte = modelo == "57"

    ns_soap  = "http://www.w3.org/2003/05/soap-envelope"
    ns_xsi   = "http://www.w3.org/2001/XMLSchema-instance"
    ns_xsd   = "http://www.w3.org/2001/XMLSchema"
    ns_nf    = "http://www.portalfiscal.inf.br/nfe"
    ns_ct    = "http://www.portalfiscal.inf.br/cte"

    body_ns  = ns_ct if is_cte else ns_nf
    xserv    = "CONSULTAR"
    tag_cons = "consSitCTe" if is_cte else "consSitNFe"
    tag_chave = "chCTe" if is_cte else "chNFe"

    env = etree.Element(
        f"{{{ns_soap}}}Envelope",
        nsmap={"soap12": ns_soap, "xsi": ns_xsi, "xsd": ns_xsd},
    )

    header = etree.SubElement(env, f"{{{ns_soap}}}Header")
    cabec  = etree.SubElement(
        header, "nfeCabecMsg", nsmap={None: wsdl_ns}
    )
    etree.SubElement(cabec, "cUF").text   = c_uf
    etree.SubElement(cabec, "versaoDados").text = "4.00"

    body  = etree.SubElement(env, f"{{{ns_soap}}}Body")
    dados = etree.SubElement(body, "nfeDadosMsg", nsmap={None: wsdl_ns})
    cons  = etree.SubElement(
        dados, tag_cons,
        versao="4.00",
        nsmap={None: body_ns},
    )
    etree.SubElement(cons, "tpAmb").text  = tp_amb
    etree.SubElement(cons, "xServ").text  = xserv
    etree.SubElement(cons, tag_chave).text = chave44

    return etree.tostring(env, xml_declaration=True, encoding="utf-8")


def _parse_soap_response(xml_bytes: bytes) -> tuple[str, str]:
    """Extrai cStat e xMotivo da resposta SOAP da SEFAZ."""
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return "999", "Resposta XML inválida da SEFAZ"

    # Namespaces possíveis na resposta
    namespaces = [
        "http://www.portalfiscal.inf.br/nfe",
        "http://www.portalfiscal.inf.br/cte",
    ]
    for ns in namespaces:
        c_stat_el  = root.find(f".//{{{ns}}}cStat")
        x_motivo_el = root.find(f".//{{{ns}}}xMotivo")
        if c_stat_el is not None:
            return (
                c_stat_el.text.strip(),
                x_motivo_el.text.strip() if x_motivo_el is not None else "–",
            )

    # Fallback sem namespace
    c_stat_el  = root.find(".//cStat")
    x_motivo_el = root.find(".//xMotivo")
    if c_stat_el is not None:
        return (
            c_stat_el.text.strip(),
            x_motivo_el.text.strip() if x_motivo_el is not None else "–",
        )

    return "999", "cStat não encontrado na resposta"


# ── Endpoint ───────────────────────────────────────────────────────────────────

@router.post("/api/sefaz-consulta", response_model=SefazConsultaResponse)
async def sefaz_consulta(req: SefazConsultaRequest):
    """
    Consulta a situação de uma NF-e, NFC-e ou CT-e na SEFAZ.

    Parâmetro:
        chave  – chave de acesso de 44 dígitos (sem espaços ou pontuação)

    Retorno:
        cStat   – código de status SEFAZ (ex: "100" = Autorizada)
        xMotivo – descrição do status (ex: "Uso Autorizado")
        tpAmb   – ambiente ("1" = Produção, "2" = Homologação)
        modelo  – modelo do documento ("55" = NF-e, "65" = NFC-e, "57" = CT-e)
    """
    chave = req.chave.strip().replace(" ", "")

    if len(chave) != 44 or not chave.isdigit():
        raise HTTPException(
            status_code=422,
            detail="Chave de acesso deve conter exatamente 44 dígitos numéricos.",
        )

    c_uf   = chave[:2]
    tp_amb = chave[20]
    modelo = chave[20:22]

    url, soap_action, wsdl_ns = _get_endpoint(chave)
    soap_body = _build_soap_nfe(chave, tp_amb, c_uf, wsdl_ns)

    headers = {
        "Content-Type": "application/soap+xml; charset=utf-8",
        "SOAPAction": soap_action,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0, verify=True) as client:
            response = await client.post(url, content=soap_body, headers=headers)
            response.raise_for_status()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Timeout ao consultar SEFAZ.")
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502,
            detail=f"SEFAZ retornou HTTP {e.response.status_code}.",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Erro de conexão com SEFAZ: {e}")

    c_stat, x_motivo = _parse_soap_response(response.content)

    return SefazConsultaResponse(
        cStat=c_stat,
        xMotivo=x_motivo,
        tpAmb=tp_amb,
        modelo=modelo,
    )
