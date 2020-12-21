package com.michilla.dao.OHSAS;

import com.michilla.beans.OHSAS.OHSASPrograma;
import com.michilla.beans.OHSAS.OHSASTipoPrograma;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASProgramaDAO
{
    /*Crea un nuevo programa*/
    public static void create(String hostBD,
                              String usuarioBD,
                              String passwordBD,
                              String codigoEmpresa,
                              String codigoPrograma,
                              String descripcionPrograma,
                              String tipoProgrma,
                              String vigenciaPrograma) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoPrograma);
            pstmt.setString(3,descripcionPrograma);
            pstmt.setString(4,vigenciaPrograma);
            pstmt.setString(5,tipoProgrma);
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
    /*Actualiza los datos de un programa*/
    public static void update(String hostBD,
                              String usuarioBD,
                              String passwordBD,
                              String codigoEmpresa,
                              String codigoPrograma,
                              String descripcionPrograma,
                              String tipoProgrma,
                              String vigenciaPrograma) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,vigenciaPrograma);
            pstmt.setString(2,codigoEmpresa);
            pstmt.setString(3,codigoPrograma);
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
    /*Elimina un programa*/
    public static void delete(String hostBD,
                              String usuarioBD,
                              String passwordBD,
                              String codigoEmpresa,
                              String codigoPrograma) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoPrograma);
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
    /*???*/
    public static List<OHSASPrograma> listaProgramaSeleccion(String hostBD, 
                                                             String usuarioBD, 
                                                             String passwordBD,
                                                             String codigoEmpresa,
                                                             String codigoArea)
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
            pstmt.setString(2,codigoArea);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASPrograma p = new OHSASPrograma();
                p.setIdPrograma(resultado.getString("ID_PROGRAMA"));  
                p.setDescripcion(resultado.getString("DESCRIPCION"));
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
    /*Entrega la cuenta de los programas existentes*/
    public static int cuentaProgramaLike(String hostBD,
                                         String usuarioBD, 
                                         String passwordBD,
                                         String codigoEmpresa,
                                         String textoEntrada,
                                         String opcionColumna)
    {
        int cuenta= 0;
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,"%" + textoEntrada + "%");
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                cuenta = Integer.parseInt(resultado.getString("CUENTA"));
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR: " +e.toString());
        }
        return cuenta;
    }
    public static List<OHSASPrograma> listaProgramaLike(String hostBD,
                                                String usuarioBD, 
                                                String passwordBD,
                                                String codigoEmpresa,
                                                String textoEntrada,
                                                int desde,
                                                int hasta,
                                                String columna,
                                                String orden,
                                                String opcionColumna)
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
            pstmt.setInt(2,desde);
            pstmt.setInt(3,hasta);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASPrograma p = new OHSASPrograma();
                p.setIdPrograma(resultado.getString("ID_PROGRAMA"));
                p.setDescripcion(resultado.getString("DESCRIPCION"));
                OHSASTipoPrograma tp = new OHSASTipoPrograma();
                tp.setIdTipoPrograma(resultado.getString("ID_TIPO_PROGRAMA"));
                tp.setDescripcion(resultado.getString("DESCRIPCION_TIPO"));
                p.setTipo(tp);
                p.setVigencia(resultado.getString("VIGENCIA"));
                listaPrograma.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR: " +e.toString());
        }
        return listaPrograma;
    }
    /*Obtiene un programa dado su codigo*/
    public static OHSASPrograma getPrograma(String hostBD,
                                            String usuarioBD,
                                            String passwordBD,
                                            String codigoEmpresa,
                                            String idPrograma)
    {
        OHSASPrograma programa = new OHSASPrograma();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            ResultSet resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                programa.setIdPrograma(resultado.getString("ID_PROGRAMA"));
                programa.setDescripcion(resultado.getString("DESCRIPCION"));
                OHSASTipoPrograma tp = new OHSASTipoPrograma();
                tp.setIdTipoPrograma(resultado.getString("ID_TIPO_PROGRAMA"));
                tp.setDescripcion(resultado.getString("DESCRIPCION_TIPO"));
                tp.setRevision(resultado.getString("REVISION"));
                programa.setTipo(tp);
                programa.setVigencia(resultado.getString("VIGENCIA"));
            }
            pstmt.close();
            resultado.close();
            /*parte que obtiene las observaciones*/
            List<String> observaciones = new ArrayList<String>();
            
            String obtieneObservaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            pstmt = conn.prepareStatement(obtieneObservaciones);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,programa.getTipo().getIdTipoPrograma());
            resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                observaciones.add(resultado.getString("OBSERVACION"));
            }
            programa.getTipo().setObservaciones(observaciones);
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return programa;
    }
    public static List<OHSASPrograma> listaPrograma(String hostBD, 
                                                    String usuarioBD, 
                                                    String passwordBD,
                                                    String codigoEmpresa
                                                   )
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
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASPrograma p = new OHSASPrograma();
                p.setIdPrograma(resultado.getString("ID_PROGRAMA"));  
                p.setDescripcion(resultado.getString("DESCRIPCION"));
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
}
