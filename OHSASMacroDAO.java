package com.michilla.dao.OHSAS;

import com.michilla.beans.OHSAS.OHSASMacro;
import com.michilla.beans.OHSAS.OHSASOperacion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASMacroDAO
{
    /*Crea una nueva macro operacion*/
    public static void create(String hostBD,
                       String usuarioBD,
                       String passwordBD,
                       String codigoEmpresa,
                       String codigoMacro,
                       String descripcionMacro, 
                       String vigenciaMacro,
                       String idPrograma,
                       String fechaInicio,
                       String fechaFin,
                       String codigoCentroCosto) throws SQLException
    {
    
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */

            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoMacro);
            pstmt.setString(3,idPrograma);
            pstmt.setString(4,fechaInicio);
            pstmt.setString(5,fechaFin);
            pstmt.setString(6,codigoCentroCosto);
            pstmt.setString(7,descripcionMacro);
            pstmt.setString(8,vigenciaMacro);
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
    /*Crea una nueva macro operacion*/
    public static void update(String hostBD,
                       String usuarioBD,
                       String passwordBD,
                       String codigoEmpresa,
                       String codigoMacro,
                       String descripcionMacro, 
                       String vigenciaMacro,
                       String idPrograma,
                       String fechaInicio,
                       String fechaFin,
                       String codigoCentroCosto) throws SQLException
    {
    
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */

            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,descripcionMacro);
            pstmt.setString(2,codigoEmpresa);
            pstmt.setString(3,codigoMacro);
            pstmt.setString(4,idPrograma);
            pstmt.setString(5,fechaInicio);
            pstmt.setString(6,fechaFin);
            pstmt.setString(7,codigoCentroCosto);
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
    /*Elimina una macro operacion*/
    public static void delete(String hostBD,
                       String usuarioBD,
                       String passwordBD,
                       String codigoEmpresa, 
                       String codigoMacroOp,
                       String idPrograma,
                       String fechaInicio,
                       String fechaFin,
                       String codigoCentroCosto) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String a = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */

            PreparedStatement pstmt = conn.prepareStatement(a);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoMacroOp);
            pstmt.setString(3,idPrograma);
            pstmt.setString(4,fechaInicio);
            pstmt.setString(5,fechaFin);
            pstmt.setString(6,codigoCentroCosto);
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
    /*Obtiene un listado de macro actividades con sus respectivas actividades*/
    public static List<OHSASMacro> getListaMacro(String hostBD, 
                                                 String usuarioBD, 
                                                 String passwordBD, 
                                                 String codigoEmpresa, 
                                                 String idPrograma,
                                                 String codigoCentroCosto
                                                )
    {

        List<OHSASMacro> listaMacro = new ArrayList<OHSASMacro>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            /*Primero obtenemos las macro operaciones*/

            String obtieneDatosMacro = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(obtieneDatosMacro);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,codigoCentroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASMacro o = new OHSASMacro();
                o.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                o.setDescripcionMacro(resultado.getString("DESCRIPCION"));
                listaMacro.add(o);
            }
            pstmt.close();
            resultado.close();
            /*Luego, por cada macro operacion obtenida, sacamos sus operaciones*/
            for(OHSASMacro o : listaMacro)
            {
                List<OHSASOperacion> listadoActividad = new ArrayList<OHSASOperacion>();
                
                String obtieneOperaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(obtieneOperaciones);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,o.getIdMacro());
                pstmt.setString(3,idPrograma);
                pstmt.setString(4,codigoCentroCosto);
                resultado = pstmt.executeQuery();
                while (resultado.next())
                {
                    OHSASOperacion a = new OHSASOperacion();
                    a.setIdActividad(resultado.getString("ID_OPERACION"));
                    a.setDescripcion(resultado.getString("DESCRIPCION"));
                    listadoActividad.add(a);
                }
                o.setListaActividades(listadoActividad);
                pstmt.close();
                resultado.close();
            }
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " + e.toString());
        }
        return listaMacro;
    }
    /*Obtiene una macroactividad*/
    public static OHSASMacro getMacro(String hostBD, 
                                                          String usuarioBD, 
                                                          String passwordBD,
                                                          String codigoEmpresa,
                                                          String codigoMacro,
                                                          String codigoPrograma,
                                                          String codigoArea)
    {
        OHSASMacro macro = new OHSASMacro();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoMacro);
            pstmt.setString(3,codigoPrograma);
            pstmt.setString(4,codigoArea);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                macro.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                macro.setDescripcionMacro(resultado.getString("DESCRIPCION"));
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return macro;
    }
    /*Obtiene un programa con sus macro operaciones, operaciones y registros*/
    public static List<OHSASMacro> listaRegistro(String hostBD, 
                                            String usuarioBD, 
                                            String passwordBD,
                                            String codigoEmpresa,
                                            String idPrograma,
                                            String fechaInicio,
                                            String fechaFin,
                                            String codigoCentroCosto
                                           )
    {
        List<OHSASMacro> listaMacro = new ArrayList<OHSASMacro>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            /*Primero, se obtienen las macro operaciones del programa*/
            
            String obtieneMacroOperaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(obtieneMacroOperaciones);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                OHSASMacro m = new OHSASMacro();
                m.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                m.setDescripcionMacro(resultado.getString("DESCRIPCION"));
                listaMacro.add(m);
            }
            resultado.close();
            pstmt.close();
            /*Luego, se obtienen las operaciones de las macro*/
            for (OHSASMacro m : listaMacro)
            {
                String obtieneOperaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(obtieneOperaciones);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,idPrograma);
                pstmt.setString(3,m.getIdMacro());
                pstmt.setString(4,fechaInicio);
                pstmt.setString(5,fechaFin);
                pstmt.setString(6,codigoCentroCosto);
                resultado = pstmt.executeQuery();
                List<OHSASOperacion> listaOperacion = new ArrayList<OHSASOperacion>();
                while(resultado.next())
                {
                    OHSASOperacion o = new OHSASOperacion();
                    o.setIdActividad(resultado.getString("ID_OPERACION"));
                    o.setDescripcion(resultado.getString("DESCRIPCION"));
                    o.setPeriodo(resultado.getInt("PERIODO"));
                    o.setNumeroPersona(resultado.getString("CADENA_PERSONAL"));
                    listaOperacion.add(o);
                    
                }
                m.setListaActividades(listaOperacion);
                pstmt.close();
                resultado.close();
            }
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return listaMacro;
    }
    
    public static List<OHSASMacro> getListaMacroSeleccion(String hostBD, 
                                                 String usuarioBD, 
                                                 String passwordBD, 
                                                 String codigoEmpresa, 
                                                 String codigoCentroCosto
                                                )
    {

        List<OHSASMacro> listaMacro = new ArrayList<OHSASMacro>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            /*Primero obtenemos las macro operaciones*/

            String obtieneDatosMacro = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(obtieneDatosMacro);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoCentroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASMacro o = new OHSASMacro();
                o.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                o.setDescripcionMacro(resultado.getString("DESCRIPCION"));
                listaMacro.add(o);
            }
            pstmt.close();
            resultado.close();
            /*Luego, por cada macro operacion obtenida, sacamos sus operaciones*/
            for(OHSASMacro o : listaMacro)
            {
                List<OHSASOperacion> listadoActividad = new ArrayList<OHSASOperacion>();
                
                String obtieneOperaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(obtieneOperaciones);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,o.getIdMacro());
                pstmt.setString(3,codigoCentroCosto);
                resultado = pstmt.executeQuery();
                while (resultado.next())
                {
                    OHSASOperacion a = new OHSASOperacion();
                    a.setIdActividad(resultado.getString("ID_OPERACION"));
                    a.setDescripcion(resultado.getString("DESCRIPCION"));
                    listadoActividad.add(a);
                }
                o.setListaActividades(listadoActividad);
                pstmt.close();
                resultado.close();
            }
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " + e.toString());
        }
        return listaMacro;
    }
}
