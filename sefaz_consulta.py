"""
SEFAZ – Validação de NF-e / NFC-e / CT-e a partir do próprio XML
─────────────────────────────────────────────────────────────────
Os webservices de consulta da SEFAZ exigem certificado digital A1
(mTLS). Como alternativa segura e sem dependências externas, este
endpoint valida o documento a partir dos campos presentes no XML:

  • nProt   → número do protocolo de autorização (cStat 100 ou 101)
  • cStat   → código de status dentro do XML (infProt ou infCanc)
  • xMotivo → descrição do status
  • dhRecbto → data/hora do recebimento na SEFAZ

Se o XML contém nProt + cStat 100/101, a NF-e foi autorizada/
cancelada pela SEFAZ — o protocolo É a prova de autorização.

No seu main.py:
    from sefaz_consulta import router as sefaz_router
    app.include_router(sefaz_router)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import xml.etree.ElementTree as ET
import re

router = APIRouter()


class SefazConsultaRequest(BaseModel):
    chave: str            # chave de 44 dígitos
    xml_content: str = "" # conteúdo do XML em string (opcional mas recomendado)


class SefazConsultaResponse(BaseModel):
    cStat:    str
    xMotivo:  str
    tpAmb:    str
    modelo:   str
    nProt:    str = ""
    dhRecbto: str = ""
    fonte:    str = "xml"  # "xml" ou "sefaz"


# ── helpers ───────────────────────────────────────────────────────────────────

_NS = re.compile(r'\{[^}]+\}')

def _strip(tag: str) -> str:
    return _NS.sub('', tag)

def _find(root, *tags) -> str:
    for tag in tags:
        for el in root.iter():
            if _strip(el.tag) == tag:
                return (el.text or '').strip()
    return ''

def _chave_info(chave44: str) -> dict:
    if len(chave44) != 44:
        return {}
    return {
        'cUF':   chave44[:2],
        'tpAmb': chave44[20],
        'modelo': chave44[20:22],
    }

# mapa cStat → status legível
_STATUS = {
    '100': ('autorizada', '✓ Autorizada'),
    '101': ('cancelada',  '✗ Cancelada'),
    '110': ('denegada',   '✗ Denegada'),
    '150': ('autorizada', '✓ Autorizada (fora de prazo)'),
    '151': ('cancelada',  '✗ Cancelada (fora de prazo)'),
    '301': ('denegada',   '✗ Denegada – irreg. emitente'),
    '302': ('denegada',   '✗ Denegada – irreg. destinatário'),
}

def _status(cStat: str):
    return _STATUS.get(cStat, ('erro', f'cStat {cStat}'))


# ── validação a partir do XML ─────────────────────────────────────────────────

def _validar_xml(xml_str: str, chave44: str) -> SefazConsultaResponse:
    info = _chave_info(chave44)
    tpAmb  = info.get('tpAmb', '1')
    modelo = info.get('modelo', '55')

    try:
        root = ET.fromstring(xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str)
    except ET.ParseError:
        return SefazConsultaResponse(
            cStat='999', xMotivo='XML inválido ou corrompido',
            tpAmb=tpAmb, modelo=modelo, fonte='xml'
        )

    # Procura protocolo de autorização (infProt) ou cancelamento (infCanc / retCancNFe)
    cStat    = _find(root, 'cStat')
    xMotivo  = _find(root, 'xMotivo')
    nProt    = _find(root, 'nProt')
    dhRecbto = _find(root, 'dhRecbto')

    # tpAmb pode estar no XML também
    tpAmb_xml = _find(root, 'tpAmb')
    if tpAmb_xml:
        tpAmb = tpAmb_xml

    if not cStat:
        # XML sem protocolo: pode ser XML de envio sem retorno anexado
        # Verifica se a chave bate com o Id do infNFe
        ch_xml = ''
        for el in root.iter():
            if _strip(el.tag) == 'infNFe':
                ch_xml = el.get('Id', '').replace('NFe', '')
                break
        if ch_xml == chave44:
            return SefazConsultaResponse(
                cStat='999',
                xMotivo='XML sem protocolo de autorização (nProt ausente). '
                        'Certifique-se de usar o XML com o protocolo SEFAZ anexado.',
                tpAmb=tpAmb, modelo=modelo, nProt='', fonte='xml'
            )
        return SefazConsultaResponse(
            cStat='999', xMotivo='Protocolo SEFAZ não encontrado no XML.',
            tpAmb=tpAmb, modelo=modelo, fonte='xml'
        )

    status_cls, label = _status(cStat)

    # Enriquece xMotivo com nProt e data
    detalhe = ''
    if nProt:
        detalhe += f' · Prot: {nProt}'
    if dhRecbto:
        detalhe += f' · {dhRecbto[:10]}'

    return SefazConsultaResponse(
        cStat=cStat,
        xMotivo=f'{xMotivo}{detalhe}',
        tpAmb=tpAmb,
        modelo=modelo,
        nProt=nProt,
        dhRecbto=dhRecbto,
        fonte='xml',
    )


# ── endpoint ──────────────────────────────────────────────────────────────────

@router.post("/api/sefaz-consulta", response_model=SefazConsultaResponse)
async def sefaz_consulta(req: SefazConsultaRequest):
    """
    Valida NF-e / NFC-e / CT-e a partir do conteúdo do próprio XML.

    O XML autorizado pela SEFAZ contém o protocolo (nProt + cStat) anexado.
    Esse protocolo é a prova oficial de autorização — não é necessário
    consultar o webservice da SEFAZ separadamente.

    Parâmetros:
        chave       – chave de acesso de 44 dígitos
        xml_content – conteúdo do XML como string (recomendado)
    """
    chave = req.chave.strip().replace(' ', '')

    if len(chave) != 44 or not chave.isdigit():
        raise HTTPException(422, "Chave deve ter 44 dígitos numéricos.")

    if req.xml_content:
        return _validar_xml(req.xml_content, chave)

    # Sem XML: informa que é necessário
    raise HTTPException(
        400,
        "Envie o conteúdo do XML no campo xml_content para validação offline. "
        "A consulta direta à SEFAZ exige certificado digital A1."
    )
