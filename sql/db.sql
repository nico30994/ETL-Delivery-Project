DROP TABLE IF EXISTS Roster;

create table Roster(
    Employ_ID float,
    Nombre text,
    DNI int,
    Lev text,
    c_Name text,
    cod_area float,
    num_tel text,
    col_prov text,
    col_loca text,
    nombre_cal text,
    altura_direc float,
    col_pi float,
    col_dp float,
    cod_postal float,
    col_com text,
    col_b_cerrado float,
    col_Sseguridad float,
    new_dir float,
    new_prov text,
    new_loc text,
    new_cal text,
    new_alt float,
    new_dp float,
    new_ps float,
    new_z_code float,
    new_entre_calles text,
    new_tel text,
    _new_comen float,
    _motivo_de_mod float,
    _col_t_area float,
    _col_t_num float,
    FORMATO text,
    RUTEADO text,
    ESTADO float,
    FECHA_DE_ENTREGA date,
    REMITO float,
    PROVEEDOR float,
    CambioPorRotura float);
    

DROP TABLE IF EXISTS Status_roster;

create table Status_roster(
    col_DNI float,
    col_ruteado text,
    col_estado text,
    col_fecha_entrega date,
    col_nro_rto float,
    col_proveedor text);

