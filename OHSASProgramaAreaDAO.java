package com.michilla.dao.OHSAS;

import com.michilla.beans.CentroCosto;
import com.michilla.beans.OHSAS.OHSASMes;
import com.michilla.beans.OHSAS.OHSASPrograma;
import com.michilla.beans.OHSAS.OHSASTipoPrograma;
import com.michilla.beans.Personal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASProgramaAreaDAO
{
    /*Crea un nuevo vinculo entre un programa y un centro costo*/
    public static void create(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String idPrograma,
        String codigoCentroCosto,String fechaInicio,String fechaFin,List<String> listaRut,List<String> listaRutResp) 
        throws SQLException
    {
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        Connection conn = DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
        conn.setAutoCommit(false);
        try
        {
            /*parte que inserta el programa y el area*/
            
            String insertaProgramaArea = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(insertaProgramaArea);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.executeUpdate();
            pstmt.close();
            /*parte que inserta el orden de las personas*/
            int numero = 1;
            for (int i = 0; i < listaRut.size();i++)
            {
                String insertaOrden = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(insertaOrden);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,idPrograma);
                pstmt.setString(3,fechaInicio);
                pstmt.setString(4,fechaFin);
                pstmt.setString(5,codigoCentroCosto);
                pstmt.setString(6,listaRut.get(i));
                pstmt.setInt(7,numero++);
                pstmt.setString(8,listaRutResp.get(i));
                pstmt.executeUpdate();
                pstmt.close();
            }
            conn.commit();
        }
        catch (SQLException e)
        {
            conn.rollback();
            System.out.println("SMPR OHSAS: " +e.toString());
            throw e;
        }
        finally 
        {
            conn.close();
        }
    }
    /*actualiza un nuevo vinculo entre un programa y un centro costo*/
    public static void update(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String idPrograma,
        String codigoCentroCosto,String fechaInicio,String fechaFin,List<String> listaRut,List<String> listaRutResp)
        throws SQLException
    {
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        Connection conn = 
            DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
        conn.setAutoCommit(false);
        try
        {
            String eliminaLista = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(eliminaLista);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.executeUpdate();
            pstmt.close();
            /*parte que inserta el orden de las personas*/
            int numero = 1;
            for (int i = 0; i < listaRut.size();i++)
            {
                String insertaOrden = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(insertaOrden);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,idPrograma);
                pstmt.setString(3,fechaInicio);
                pstmt.setString(4,fechaFin);
                pstmt.setString(5,codigoCentroCosto);
                pstmt.setString(6,listaRut.get(i));
                pstmt.setInt(7,numero++);
                pstmt.setString(8,listaRutResp.get(i));
                pstmt.executeUpdate();
                pstmt.close();
            }
            conn.commit();
        }
        catch (SQLException e)
        {
            conn.rollback();
            System.out.println("SMPR OHSAS: " +e.toString());
            throw e;
        }
        finally 
        {
            conn.close();
        }
    }
    /*Elimina el vinculo entre un programa y un centro costo*/
    public static void delete(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String idPrograma,
        String fechaInicio,String fechaFin, String codigoCentroCosto) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String a = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(a);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
            throw e;
        }
    }
    public static List<Personal> listaPersonalNumerado(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String idPrograma,String fechaInicio,String fechaFin,String codigoCentroCosto)
    {
        List<Personal> listaTrabajadores = new ArrayList<Personal>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                Personal p = new Personal();
                p.setRut(resultado.getString("RUT"));
                p.setNumeroOHSAS(resultado.getInt("NUMERO"));
                p.setNombres(resultado.getString("NOMBRES"));
                p.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                p.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                listaTrabajadores.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return listaTrabajadores;
    }
    
    /*Obtiene la lista de todo el personal de un area que tiene un numero asignado en un programa, si no tiene, numero 
     * asignado, obtiene solamente el rut*/
    public static List<Personal> listaPersonalNumeradoTodo(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String idPrograma,String fechaInicio,String fechaFin,String codigoCentroCosto)
    {
        List<Personal> listaTrabajadores = new ArrayList<Personal>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.setString(6,codigoEmpresa);
            pstmt.setString(7,idPrograma);
            pstmt.setString(8,fechaInicio);
            pstmt.setString(9,fechaFin);
            pstmt.setString(10,codigoCentroCosto);
            pstmt.setString(11,codigoCentroCosto);
            pstmt.setString(12,codigoEmpresa);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                Personal p = new Personal();
                p.setRut(resultado.getString("RUT"));
                p.setNumeroOHSAS(resultado.getInt("NUMERO"));
                p.setNombres(resultado.getString("NOMBRES"));
                p.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                p.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                listaTrabajadores.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSASProgramaAreaDAO/listaPersonalNumeradoTodo: " + e.toString());
        }
        return listaTrabajadores;
    }
    public static List<Personal> listaPersonalNumeradoRevisor(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String idPrograma,String fechaInicio,String fechaFin,String codigoCentroCosto)
    {
        List<Personal> listaTrabajadores = new ArrayList<Personal>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.setString(6,codigoEmpresa);
            pstmt.setString(7,idPrograma);
            pstmt.setString(8,fechaInicio);
            pstmt.setString(9,fechaFin);
            pstmt.setString(10,codigoCentroCosto);
            pstmt.setString(11,codigoCentroCosto);
            pstmt.setString(12,codigoEmpresa);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                Personal p = new Personal();
                p.setRut(resultado.getString("REVISOR"));
                p.setNombres(resultado.getString("NOMBRES_REViSOR"));
                p.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO_REVISOR"));
                p.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO_REVISOR"));
                p.setRevisorOHSAS(resultado.getString("CHECK_REVISOR"));
                listaTrabajadores.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSASProgramaAreaDAO/listaPersonalNumeradoRevisor: " + e.toString());
        }
        return listaTrabajadores;
    }
    /*Obtiene un programa dado su codigo*/
    public static OHSASPrograma getProgramaArea(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String idPrograma,String fechaInicio,String fechaFin,String codigoCentroCosto)
    {
        OHSASPrograma programa = new OHSASPrograma();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
           
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                programa.setIdPrograma(resultado.getString("ID_PROGRAMA"));
                programa.setDescripcion(resultado.getString("DESCRIPCION"));
                CentroCosto cc = new CentroCosto();
                cc.setCodigo(resultado.getInt("CODIGO_CENTRO_COSTO"));
                cc.setDescripcion(resultado.getString("DESCRIPCION_COSTO"));
                programa.setCentroCosto(cc);
                OHSASMes fechaInicioMes = new OHSASMes();
                fechaInicioMes.setFecha(resultado.getString("FECHA_INICIO"));
                programa.setFechaInicio(fechaInicioMes);
                OHSASMes fechaFinMes = new OHSASMes();
                fechaFinMes.setFecha(resultado.getString("FECHA_FIN"));
                programa.setFechaFin(fechaFinMes);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " + e.toString());
        }
        return programa;
    }
    public static List<OHSASPrograma> listaProgramaFechaArea(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String opcionArea,String opcionPrograma)
    {
        List<OHSASPrograma> listaPrograma = new ArrayList<OHSASPrograma>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,opcionPrograma);
            pstmt.setString(3,opcionArea);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASPrograma p = new OHSASPrograma();
                p.setIdPrograma(resultado.getString("ID_PROGRAMA"));  
                p.setDescripcion(resultado.getString("DESCRIPCION"));
                OHSASMes fechaInicio = new OHSASMes();
                fechaInicio.setMes(resultado.getString("FECHA_INICIO_MES"));
                fechaInicio.setFecha(resultado.getString("FECHA_INICIO"));
                p.setFechaInicio(fechaInicio);
                OHSASMes fechaFin = new OHSASMes();
                fechaFin.setMes(resultado.getString("FECHA_FIN_MES"));
                fechaFin.setFecha(resultado.getString("FECHA_FIN"));
                p.setFechaFin(fechaFin);
                CentroCosto cc = new CentroCosto();
                cc.setCodigo(resultado.getInt("CODIGO_CENTRO_COSTO"));
                p.setCentroCosto(cc);
                listaPrograma.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return listaPrograma;
    }
    public static List<Personal> listaPersonalCentroCosto(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String centroCosto,String idPrograma)
    {
        List<Personal> listaTrabajadores = new ArrayList<Personal>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,centroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                Personal t = new Personal();
                t.setRut(resultado.getString("RUT"));
                t.setNombres(resultado.getString("NOMBRES"));
                t.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                t.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                listaTrabajadores.add(t);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/PersonalDAO/listaPersonalCentroCosto: " + e.toString());
        }
        return listaTrabajadores;
    }
}
