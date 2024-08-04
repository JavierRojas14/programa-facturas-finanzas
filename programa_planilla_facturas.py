"""
Este es un programa para generar la planilla de Control de Facturas. Unidad de Finanzas.
Javier Rojas Benítez
"""

import time
import datetime
import json
import os
import glob

import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'

COLUMNAS_SII_REGISTRO_O_NO_INCLUIR = {
    "Tipo Doc": int,
    "RUT Proveedor": str,
    "Razon Social": str,
    "Folio": int,
    "Fecha Docto": str,
    "Fecha Recepcion": str,
    "Fecha Acuse": str,
    "Monto Exento": "Int64",
    "Monto Neto": "Int64",
    "Monto IVA Recuperable": "Int64",
    "Monto Total": "Int64",
}

COLUMNAS_SII_PENDIENTES = {
    "Tipo Doc": int,
    "RUT Proveedor": str,
    "Razon Social": str,
    "Folio": int,
    "Fecha Docto": str,
    "Fecha Recepcion": str,
    "Monto Exento": "Int64",
    "Monto Neto": "Int64",
    "Monto IVA Recuperable": "Int64",
    "Monto Total": "Int64",
}

COLUMNAS_SII_RECLAMADAS = {
    "Tipo Doc": int,
    "RUT Proveedor": str,
    "Razon Social": str,
    "Folio": int,
    "Fecha Docto": str,
    "Fecha Recepcion": str,
    "Fecha Reclamo": str,
    "Monto Exento": "Int64",
    "Monto Neto": "Int64",
    "Monto IVA Recuperable": "Int64",
    "Monto Total": "Int64",
}


COLUMNAS_ACEPTA = {
    "tipo": int,
    "folio": int,
    "emisor": str,
    "publicacion": str,
    "estado_acepta": str,
    "estado_sii": str,
    "referencias": str,
    "estado_nar": str,
    "estado_devengo": str,
    "folio_oc": str,
    "folio_rc": str,
    "fecha_ingreso_rc": str,
    "folio_sigfe": "Int64",
    "tarea_actual": str,
    "estado_cesion": str,
}


class GeneradorPlanillaFinanzas:
    """
    Esta es la clase madre que permite obtener la planilla de control de facturas.
    Consta de 6 funciones principales.
    """

    def __init__(self):
        pass

    def correr_programa(self):
        """
        Esta función permite correr el programa de para obtener el cruce de bases de datos
        SII - ACEPTA - SIGFE - TURBO - SCI de facturas. Ejectura los siguientes pasos:

        - Cargar archivos (la función más costosa)
        - Unir los archivos según la llave RUT-DV + Folio SII
        - Calcular el tiempo entre que se recibe la factura desde el SII y la fecha actual.
        - Obtener las referencias entre Notas de Créditos y Facturas.
        - Filtrar columnas innecesarias, y solo dejar las columnas necesarias
        """
        start_time = time.time()
        leer = input(
            "¿Quieres leer los archivos de este año o todos los años? \n"
            "1) Este año \n"
            "2) Todos los años \n"
            "> "
        )

        archivos_facturas = self.obtener_archivos("facturas", leer)
        tablas_de_facturas = self.obtener_facturas_base_de_datos(archivos_facturas)

        archivos_oc = self.obtener_archivos("oc", leer)
        oc_limpias = self.obtener_oc_base_de_datos(archivos_oc)

        archivo_articulos = self.obtener_archivos("articulos", leer)
        articulos = self.obtener_articulos_base_de_datos(archivo_articulos)

        facturas_unidas = self.unir_dfs(tablas_de_facturas)

        facturas_cumplen_tiempo = self.calcular_tiempo_8_dias(facturas_unidas)
        facturas_con_ref_nc = self.obtener_referencias_nc(facturas_cumplen_tiempo)
        facturas_con_oc = self.asociar_saldo_de_oc(facturas_con_ref_nc, oc_limpias["SIGFE_REPORTS"])
        facturas_con_maestro_articulos = self.asociar_maestro_articulos(
            facturas_con_oc, articulos["MAESTRO_ARTICULOS"]
        )

        facturas_con_ley_presupuesto = self.asociar_ley_presupuesto(
            facturas_con_maestro_articulos, articulos["LEY_PRESUPUESTOS"]
        )
        facturas_con_mismo_monto_sii = self.tienen_el_mismo_monto_sii_y_turbo(
            facturas_con_ley_presupuesto
        )

        facturas_con_columnas_necesarias = self.obtener_columnas_necesarias(
            facturas_con_mismo_monto_sii
        )

        self.guardar_dfs(facturas_con_columnas_necesarias, leer)
        print(f"La planilla final tiene {facturas_con_columnas_necesarias.shape[0]} documentos.")

        print("\nListo! No hubo ningún problema")
        print(f"--- {round(time.time() - start_time, 1)} seconds ---")

    def obtener_archivos(self, base_de_datos, leer):
        archivos_a_leer = {}
        base_de_datos_a_leer = f"crudos\\base_de_datos_{base_de_datos}"

        for carpeta_base_de_datos in os.listdir(base_de_datos_a_leer):
            archivos_a_leer[carpeta_base_de_datos] = []
            for archivo in os.listdir(os.path.join(base_de_datos_a_leer, carpeta_base_de_datos)):
                ruta_archivo = os.path.join(base_de_datos_a_leer, carpeta_base_de_datos, archivo)
                archivos_a_leer[carpeta_base_de_datos].append(ruta_archivo)

        hoy = datetime.date.today()
        anio_actual = str(hoy.year)

        if leer == "1":
            for base_de_datos, lista_archivos in archivos_a_leer.items():
                archivos_a_leer[base_de_datos] = [
                    archivo for archivo in lista_archivos if anio_actual in archivo
                ]

        return archivos_a_leer

    def obtener_facturas_base_de_datos(self, archivos_a_leer):
        diccionario_base_de_datos = {}
        for base_de_datos, lista_archivos in archivos_a_leer.items():
            print(f"Leyendo {base_de_datos}")
            if base_de_datos == "ACEPTA":
                df_sumada = self.leer_acepta()

            elif base_de_datos == "OBSERVACIONES":
                df_sumada = self.leer_observaciones(lista_archivos)

            elif base_de_datos == "SCI":
                df_sumada = self.leer_sci(lista_archivos)

            elif base_de_datos == "SIGFE":
                df_sumada = self.leer_sigfe(lista_archivos)

            elif base_de_datos == "SII":
                df_sumada = self.leer_sii()
                print(f"\nSII tiene {df_sumada.shape[0]} documentos totales\n")

            elif base_de_datos == "TURBO":
                df_sumada = self.leer_turbo(lista_archivos)

            df_sumada["RUT Emisor"] = (
                df_sumada["RUT Emisor"].str.replace(".", "", regex=False).str.upper().str.strip()
            )

            df_sumada["llave_id"] = df_sumada["RUT Emisor"].astype(str) + df_sumada["Folio"].astype(
                str
            )
            df_sumada = df_sumada.set_index("llave_id")

            df_sumada.columns = df_sumada.columns + f"_{base_de_datos}"
            df_sumada.columns = df_sumada.columns.str.replace(" ", "_")

            diccionario_base_de_datos[base_de_datos] = df_sumada

        return diccionario_base_de_datos

    def leer_acepta(self):
        acepta_unido = pd.concat(
            (
                pd.read_excel(archivo, usecols=list(COLUMNAS_ACEPTA.keys()), dtype=COLUMNAS_ACEPTA)
                for archivo in glob.glob("crudos/base_de_datos_facturas/ACEPTA/*.xls")
            )
        )
        acepta_unido = acepta_unido.rename(columns={"emisor": "RUT Emisor", "folio": "Folio"})

        return acepta_unido

    def leer_observaciones(self, lista_archivos):
        dfs = map(lambda x: pd.read_csv(x, encoding="utf-8", delimiter=";"), lista_archivos)
        df_sumada = pd.concat(dfs)
        df_sumada = df_sumada[["RUT_Emisor_SII", "Folio_SII", "OBSERVACION_OBSERVACIONES"]]
        df_sumada = df_sumada.rename(
            columns={
                "RUT_Emisor_SII": "RUT Emisor",
                "Folio_SII": "Folio",
                "OBSERVACION_OBSERVACIONES": "OBSERVACION",
            }
        )

        return df_sumada

    def leer_sci(self, lista_archivos):
        dfs = map(lambda x: pd.read_csv(x, delimiter=","), lista_archivos)
        df_sumada = pd.concat(dfs)
        df_sumada = df_sumada.rename(
            columns={"Rut Proveedor": "RUT Emisor", "Numero Documento": "Folio"}
        )

        df_sumada["Folio"] = df_sumada["Folio"].astype(str).str.replace(".0", "", regex=False)

        return df_sumada

    def leer_sigfe(self, lista_archivos):
        dfs = map(lambda x: pd.read_csv(x, delimiter=",", header=10), lista_archivos)
        df_sumada = pd.concat(dfs)
        df_sumada = df_sumada.dropna(subset=["Folio"])
        df_sumada = df_sumada.query('`Cuenta Contable` != "Cuenta Contable"')

        df_sumada["RUT Emisor"] = df_sumada["Principal"].str.split(" ").str[0]

        df_sumada = df_sumada.rename(columns={"Folio": "Folio_interno", "Número ": "Folio"})
        df_sumada = df_sumada.reset_index()

        df_sumada["Fecha"] = pd.to_datetime(df_sumada["Fecha"], dayfirst=True)
        df_sumada["Folio_interno"] = df_sumada["Folio_interno"].astype("Int32")

        mask_debe = df_sumada["Debe"] != "0"
        mask_haber = df_sumada["Haber"] != "0"

        df_sumada["Folio_interno PAGO"] = df_sumada[mask_debe]["Folio_interno"]
        df_sumada["Fecha PAGO"] = df_sumada[mask_debe]["Fecha"]

        df_sumada["Folio_interno DEVENGO"] = df_sumada["Folio_interno"][mask_haber]
        df_sumada["Fecha DEVENGO"] = df_sumada["Fecha"][mask_haber]

        df_sumada["RUT Emisor"] = (
            df_sumada["RUT Emisor"].str.replace(".", "", regex=False).str.upper().str.strip()
        )

        fecha_devengo_mas_antigua = df_sumada.groupby(by=["RUT Emisor", "Folio"])[
            "Fecha DEVENGO"
        ].min()
        folio_devengo = df_sumada.groupby(by=["RUT Emisor", "Folio"])["Folio_interno DEVENGO"].min()
        fecha_pago = df_sumada.groupby(by=["RUT Emisor", "Folio"])["Fecha PAGO"].min()
        folio_pago = df_sumada.groupby(by=["RUT Emisor", "Folio"])["Folio_interno PAGO"].min()
        df_sumada = pd.concat(
            [fecha_devengo_mas_antigua, folio_devengo, fecha_pago, folio_pago], axis=1
        ).reset_index()

        return df_sumada

    def lector_csv_sii(self, archivo, tipo_datos):
        return pd.read_csv(
            archivo,
            delimiter=";",
            index_col=False,
            usecols=list(tipo_datos.keys()),
            dtype=tipo_datos,
        )

    def leer_sii(self):
        # Define ruta de archivos a leer
        registro = glob.glob("crudos/base_de_datos_facturas/SII/*REGISTRO*.csv")
        no_incluir = glob.glob("crudos/base_de_datos_facturas/SII/*NO_INCLUIR*.csv")
        pendientes = glob.glob("crudos/base_de_datos_facturas/SII/*PENDIENTE*.csv")
        reclamados = glob.glob("crudos/base_de_datos_facturas/SII/*RECLAMADO*.csv")

        # Renombra Fecha Acuse a Fecha de Reclamo (6 de ~74000 documentos tienen registros)
        df_registro = (
            self.lector_csv_sii(archivo, COLUMNAS_SII_REGISTRO_O_NO_INCLUIR) for archivo in registro
        )
        df_registro = pd.concat(df_registro).rename(columns={"Fecha Acuse": "Fecha Reclamo"})
        # df_registro["tipo"] = "registro"

        # Renombra Fecha Acuse a Fecha de Reclamo (Ninguna tiene un valor)
        df_no_incluir = (
            self.lector_csv_sii(archivo, COLUMNAS_SII_REGISTRO_O_NO_INCLUIR)
            for archivo in no_incluir
        )
        df_no_incluir = pd.concat(df_no_incluir).rename(columns={"Fecha Acuse": "Fecha Reclamo"})
        # df_no_incluir["tipo"] = "no_incluir"

        # Agrega la columna Fecha de Reclamo para alinear dfs
        df_pendientes = (
            self.lector_csv_sii(archivo, COLUMNAS_SII_PENDIENTES) for archivo in pendientes
        )
        df_pendientes = pd.concat(df_pendientes)
        df_pendientes.insert(8, "Fecha Reclamo", np.nan)
        # df_pendientes["tipo"] = "pendientes"

        # Se mantiene el archivo como esta
        df_reclamados = (
            self.lector_csv_sii(archivo, COLUMNAS_SII_RECLAMADAS) for archivo in reclamados
        )
        df_reclamados = pd.concat(df_reclamados)
        # df_reclamados["tipo"] = "reclamados"

        # Une todos los tipos de documentos luego de alinear todos los dfs
        df_sumada = pd.concat([df_registro, df_no_incluir, df_pendientes, df_reclamados])
        df_sumada = df_sumada.rename(columns={"RUT Proveedor": "RUT Emisor"})

        # Pone Notas de Credito/Debito en negativo
        COLUMNAS_NEGATIVAS = ["Monto Exento", "Monto Neto", "Monto IVA Recuperable", "Monto Total"]
        mask_negativas = (df_sumada["Tipo Doc"] == 61) | (df_sumada["Tipo Doc"] == 56)
        df_sumada.loc[mask_negativas, COLUMNAS_NEGATIVAS] = (
            df_sumada.loc[mask_negativas, COLUMNAS_NEGATIVAS] * -1
        )

        # Elimina documentos duplicados con mismo tipo de doc, rut y folio (casos de facturas en
        # pendientes y registro)
        df_sumada = df_sumada.drop_duplicates(subset=["Tipo Doc", "RUT Emisor", "Folio"])

        return df_sumada

    def leer_turbo(self, lista_archivos):
        dfs = map(lambda x: pd.read_excel(x, header=3), lista_archivos)
        df_sumada = pd.concat(dfs)
        df_sumada = df_sumada.rename(
            columns={"Rut": "RUT Emisor", "Folio": "Folio_interno", "NºDoc.": "Folio"}
        )

        df_sumada["Folio"] = df_sumada["Folio"].astype(str).str.replace(".0", "", regex=False)
        df_sumada["Monto"] = df_sumada["Monto"].astype("Int64")

        return df_sumada

    def obtener_oc_base_de_datos(self, archivos_a_leer):
        diccionario_base_de_datos = {}
        for base_de_datos, lista_archivos in archivos_a_leer.items():
            print(f"Leyendo {base_de_datos}")
            if base_de_datos == "SIGFE_REPORTS":
                df_sumada = self.leer_sigfe_reports(lista_archivos)

            diccionario_base_de_datos[base_de_datos] = df_sumada

        return diccionario_base_de_datos

    def leer_sigfe_reports(self, lista_archivos):
        dfs = map(lambda x: pd.read_excel(x, header=5), lista_archivos)
        df_sumada = pd.concat(dfs)

        return df_sumada

    def obtener_articulos_base_de_datos(self, archivos_a_leer):
        diccionario_base_de_datos = {}
        for base_de_datos, lista_archivos in archivos_a_leer.items():
            print(f"Leyendo {base_de_datos}")
            if base_de_datos == "MAESTRO_ARTICULOS":
                df_sumada = self.leer_maestro_articulo(lista_archivos)

            elif base_de_datos == "LEY_PRESUPUESTOS":
                df_sumada = self.leer_ley_de_presupuestos(lista_archivos)

            diccionario_base_de_datos[base_de_datos] = df_sumada

        return diccionario_base_de_datos

    def leer_maestro_articulo(self, lista_archivos):
        dfs = map(lambda x: pd.read_excel(x, header=3), lista_archivos)
        df_sumada = pd.concat(dfs)

        return df_sumada

    def leer_ley_de_presupuestos(self, lista_archivos):
        dfs = map(pd.read_excel, lista_archivos)
        df_sumada = pd.concat(dfs)

        return df_sumada

    def unir_dfs(self, diccionario_dfs_limpias):
        """
        Esta función permite unir todas las bases de datos según la llave RUT-DV + Folio SII.
        - En este caso, se realiza un LEFT JOIN a la base de datos del SII. Esto, ya que es la base
        de las facturas.
        - El orden en que se agregan las bases de datos es: SII -> ACEPTA -> OBSERVACIONES -> SCI
        -> SIGFE -> TURBO
        """
        print("\nUniendo todas las bases de datos!")
        df_sii = diccionario_dfs_limpias.pop("SII")
        lista_dfs_secuenciales = list(diccionario_dfs_limpias.values())

        for df_derecha in lista_dfs_secuenciales:
            df_sii = pd.merge(df_sii, df_derecha, how="left", left_index=True, right_index=True)

        df_sii = df_sii[~df_sii.index.duplicated(keep="first")]

        return df_sii

    def calcular_tiempo_8_dias(self, df_unida):
        """
        Esta función permite calcular la diferencia de tiempo entre el día actual, y el día en que
        se recibió la factura ("Fecha Recepción SII").

        Este calculo solo se realiza a las facturas que NO estén devengadas.
        """
        print("Calculando los 8 días de las facturas!")
        mask_no_devengadas = pd.isna(df_unida["Fecha_DEVENGO_SIGFE"])

        df_unida["Fecha_Docto_SII"] = pd.to_datetime(
            df_unida["Fecha_Docto_SII"], dayfirst=True, format="mixed"
        )

        df_unida["Fecha_Recepcion_SII"] = pd.to_datetime(
            df_unida["Fecha_Recepcion_SII"], dayfirst=True, format="mixed"
        )

        df_unida["Fecha_Reclamo_SII"] = pd.to_datetime(
            df_unida["Fecha_Reclamo_SII"], dayfirst=True, format="mixed"
        )

        diferencia = (
            pd.to_datetime("today") - df_unida[mask_no_devengadas]["Fecha_Recepcion_SII"]
        ) + pd.Timedelta(days=1)

        df_unida["tiempo_diferencia_SII"] = round(diferencia / pd.Timedelta(days=1), 2)
        esta_al_dia = df_unida[mask_no_devengadas]["tiempo_diferencia_SII"] <= 8

        df_unida["esta_al_dia"] = esta_al_dia

        return df_unida

    def obtener_referencias_nc(self, df_izquierda):
        """
        Esta función permite obtener las referencias que contienen las Notas de Crédito, y
        agregarlas a la columna REFERENCIAS
        """
        print("Referenciando las Notas de Crédito...")
        # Copia la base de datos original, y resetea el indice para unir por la llave_id
        tmp = df_izquierda.copy().reset_index()
        # Obtiene Notas de Creditos, y elimina las que no tengan una referencia
        referencias_nc = tmp.query("Tipo_Doc_SII == 61")["referencias_ACEPTA"].dropna()
        referencias_facturas_validas = referencias_nc.apply(self.extraer_referencia_de_nc_de_json)

        # Agrega las referencias de documentos 33 como columna, y elimina los 0 iniciales
        tmp["referencias_a_facturas"] = referencias_facturas_validas.str.lstrip("0")
        tmp["referencias_a_facturas"] = tmp["RUT_Emisor_SII"] + tmp["referencias_a_facturas"]

        # Une las referencias de las NC con sus facturas originales
        referencias_a_nc = tmp.merge(
            tmp, how="inner", left_on="llave_id", right_on="referencias_a_facturas"
        )["llave_id_y"]

        tmp["referencias_a_nc"] = referencias_a_nc

        # Consolida ambas referencias cruzadas en una unica columna
        tmp["referencias_a_facturas"] = tmp["referencias_a_facturas"].fillna("")
        tmp["referencias_a_nc"] = tmp["referencias_a_nc"].fillna("")
        tmp["REFERENCIAS"] = (
            ">FE: " + tmp["referencias_a_facturas"] + "\n>NC: " + tmp["referencias_a_nc"]
        )

        # Vuelve a poner el indice como la llave_id
        tmp = tmp.set_index("llave_id")

        return tmp

    def extraer_referencia_de_nc_de_json(self, string_json):
        """
        Esta función permite obtener las referencias que tienen las Notas de Crédito dentro la
        base de datos ACEPTA
        """
        diccionario_json = json.loads(string_json, strict=False)
        for documento_referencia in diccionario_json:
            if documento_referencia["Tipo"] == "33":
                return documento_referencia["Folio"]

        return None

    def asociar_saldo_de_oc(self, df_junta, oc_sigfe):
        """
        Esta función permite agregar el saldo disponible de las ordenes de compra a cada
        factura que esté asociada.
        """
        # oc_pendientes = oc_sigfe.query('`Monto Disponible` > 0')
        # mask_subtitulo_22 = oc_pendientes['Concepto Presupuesto'].str[:2] == '22'
        # oc_pendientes_subt_22 = oc_pendientes[mask_subtitulo_22]

        print("Asociando Órdenes de Compra!")
        df_junta["Concepto_Presupuesto_OC"] = ""
        for orden_compra in oc_sigfe["Número Documento"].unique():
            if not (orden_compra in ["2022", "2"]):
                mask_oc_sigfe = oc_sigfe["Número Documento"] == orden_compra
                datos_oc = oc_sigfe[mask_oc_sigfe]
                monto_disponible = datos_oc["Monto Disponible"].iloc[0]
                numero_compromiso = datos_oc["Folio"].iloc[0]
                concepto_presupuesto = datos_oc["Concepto Presupuesto"].iloc[0]

                mask_oc_acepta = df_junta["folio_oc_ACEPTA"] == orden_compra
                facturas_asociadas = df_junta[mask_oc_acepta]

                if not facturas_asociadas.empty:
                    df_junta.loc[mask_oc_acepta, "Numero_Compromiso_OC"] = numero_compromiso
                    df_junta.loc[mask_oc_acepta, "Monto_Disponible_OC"] = monto_disponible
                    df_junta.loc[mask_oc_acepta, "Concepto_Presupuesto_OC"] = concepto_presupuesto

        return df_junta

    def asociar_maestro_articulos(self, df_junta, df_maestro_articulo):
        print("Asociando con el Maestro Articulos")
        tmp = df_junta.copy()
        tmp = tmp.reset_index()

        df_maestro_art = df_maestro_articulo.copy()
        df_maestro_art.columns = df_maestro_art.columns + "_MAESTRO_ARTICULOS"

        facturas_con_maestro_articulos = pd.merge(
            tmp,
            df_maestro_art,
            how="left",
            left_on="Codigo_Articulo_SCI",
            right_on="Código_MAESTRO_ARTICULOS",
        )

        facturas_con_maestro_articulos = facturas_con_maestro_articulos.reset_index(drop=True)

        return facturas_con_maestro_articulos

    def asociar_ley_presupuesto(self, df_junta, df_ley_presupuestos):
        print("Asociando con la Ley de Presupuestos!")
        tmp = df_junta.copy()

        ley_presupuesto = df_ley_presupuestos.copy()
        ley_presupuesto.columns = ley_presupuesto.columns + "_LEY_PRESUPUESTO"
        tmp["Items_MAESTRO_ARTICULOS"] = tmp["Items_MAESTRO_ARTICULOS"].astype(str)
        ley_presupuesto["Numero_Concepto_LEY_PRESUPUESTO"] = ley_presupuesto[
            "Numero_Concepto_LEY_PRESUPUESTO"
        ].astype(str)

        facturas_con_ley_presupuesto = pd.merge(
            tmp,
            ley_presupuesto,
            how="left",
            left_on="Items_MAESTRO_ARTICULOS",
            right_on="Numero_Concepto_LEY_PRESUPUESTO",
        )

        facturas_con_ley_presupuesto = facturas_con_ley_presupuesto.reset_index(drop=True)

        return facturas_con_ley_presupuesto

    def tienen_el_mismo_monto_sii_y_turbo(self, df_junta):
        print("Viendo si los montos de SII y TURBO coinciden...")
        tmp = df_junta.copy()

        # Indica si los montos totales de SII y TURBO (bodega) coinciden
        tmp["monto_sii_y_turbo_coinciden"] = tmp["Monto_Total_SII"] == tmp["Monto_TURBO"]

        return tmp

    def obtener_columnas_necesarias(self, df_izquierda):
        """
        Esta función selecciona sólo las columnas necesarias en el fomrato final para
        el control de facturas.

        - Utiliza 11 columnas del SII
        - Utiliza 11 columnas de ACEPTA
        - Utiliza 4 columnas de SIGFE
        - Utiliza 4 columnas de SCI
        - Utiliza 4 columnas de TURBO
        - Utiliza 4 columnas calculadas internamente.

        Además, la ordena por fecha de Docto del SII
        """
        print("Filtrando las columnas necesarias!")
        columnas_a_ocupar = [
            "llave_id",
            "Tipo_Doc_SII",
            "RUT_Emisor_SII",
            "Razon_Social_SII",
            "Folio_SII",
            "Fecha_Docto_SII",
            "Fecha_Recepcion_SII",
            "Fecha_Reclamo_SII",
            "Monto_Exento_SII",
            "Monto_Neto_SII",
            "Monto_IVA_Recuperable_SII",
            "Monto_Total_SII",
            "publicacion_ACEPTA",
            "estado_acepta_ACEPTA",
            "estado_sii_ACEPTA",
            "estado_nar_ACEPTA",
            "estado_devengo_ACEPTA",
            "folio_oc_ACEPTA",
            "Numero_Compromiso_OC",
            "Monto_Disponible_OC",
            "Concepto_Presupuesto_OC",
            "folio_rc_ACEPTA",
            "fecha_ingreso_rc_ACEPTA",
            "folio_sigfe_ACEPTA",
            "tarea_actual_ACEPTA",
            "estado_cesion_ACEPTA",
            "Fecha_DEVENGO_SIGFE",
            "Folio_interno_DEVENGO_SIGFE",
            "Fecha_PAGO_SIGFE",
            "Folio_interno_PAGO_SIGFE",
            "Fecha_Recepción_SCI",
            "Registrador_SCI",
            "Codigo_Articulo_SCI",
            "Articulo_SCI",
            "N°_Acta_SCI",
            "Familia_MAESTRO_ARTICULOS",
            "Items_MAESTRO_ARTICULOS",
            "Nombre Items_MAESTRO_ARTICULOS",
            "Cargar_en_LEY_PRESUPUESTO",
            "Ubic._TURBO",
            "NºPresu_TURBO",
            "Folio_interno_TURBO",
            "NºPago_TURBO",
            "Monto_TURBO",
            "tiempo_diferencia_SII",
            "esta_al_dia",
            "monto_sii_y_turbo_coinciden",
            "REFERENCIAS",
            "OBSERVACION_OBSERVACIONES",
        ]

        df_filtrada = df_izquierda[columnas_a_ocupar]
        df_filtrada["Tipo_Doc_SII"] = df_filtrada["Tipo_Doc_SII"].astype("category")
        df_filtrada = df_filtrada.sort_values(
            by=["Fecha_Docto_SII", "tiempo_diferencia_SII"], ascending=[True, False]
        )

        return df_filtrada

    def guardar_dfs(self, df_columnas_utiles, leer):
        """
        Esta función permite guardar la planilla de facturas para el control de Devengo.
        - El nombre del archivo es PLANILLA DE CONTROL AL  (fecha actual)
        - Se formatea automáticamente la fecha al escribirse a formato excel.
        """
        print("Guardando la planilla...")
        diccionario_nombres = {"1": pd.to_datetime("today").year, "2": "historico"}
        periodo_a_guardar = diccionario_nombres[leer]

        if periodo_a_guardar != "historico":
            df_historico = pd.read_csv(
                "control_facturas_historico.csv", sep=";", encoding="utf-8", low_memory=False
            )
            concatenado = pd.concat([df_historico, df_columnas_utiles])
            concatenado = concatenado.drop_duplicates(subset="llave_id", keep="last")
            concatenado.to_csv(
                "control_facturas_historico.csv",
                sep=";",
                decimal=",",
                encoding="utf-8",
                index=False,
            )

            self.filtrar_y_guardar_observaciones(df_columnas_utiles, periodo_a_guardar)

        else:
            df_columnas_utiles.to_csv(
                "control_facturas_historico.csv",
                sep=";",
                decimal=",",
                encoding="utf-8",
                index=False,
            )
            for año in df_columnas_utiles["Fecha_Docto_SII"].dt.year.unique():
                self.filtrar_y_guardar_observaciones(df_columnas_utiles, año)

    def filtrar_y_guardar_observaciones(self, df_columnas_utiles, periodo_a_guardar):
        df_observaciones_año = df_columnas_utiles.query(
            "Fecha_Docto_SII.dt.year == @periodo_a_guardar"
        )
        nombre_archivo = f"OBSERVACIONES {periodo_a_guardar}.csv"
        df_observaciones_año.to_csv(
            f"crudos\\base_de_datos_facturas\\OBSERVACIONES\\{nombre_archivo}",
            sep=";",
            decimal=",",
            encoding="utf-8",
            index=False,
        )


programa = GeneradorPlanillaFinanzas()
programa.correr_programa()
