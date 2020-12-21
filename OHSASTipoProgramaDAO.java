package com.michilla.dao.OHSAS;

import com.michilla.beans.OHSAS.OHSASTipoPrograma;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASTipoProgramaDAO
{
    /*Crea un nuevo programa*/
    public static void create(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String codigoTipo,
        String descripcionTipo,String dirigido,String revision,List<String> observacion) throws SQLException
    {
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        Connection conn = DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
        conn.setAutoCommit(false);
        try
        {
            String insertaTipo = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(insertaTipo);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoTipo);
            pstmt.setString(3,descripcionTipo);
            pstmt.setString(4,dirigido);
            pstmt.setString(5,revision);
            pstmt.executeUpdate();
            pstmt.close();
            for (String o : observacion)
            {
                String insertaObservacion = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(insertaObservacion);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,codigoTipo);
                pstmt.setString(3,codigoEmpresa);
                pstmt.setString(4,codigoTipo);
                pstmt.setString(5,o);
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
    /*Entrega la cuenta de los tipos de programas existentes*/
    public static int cuentaTipoProgramaLike(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String textoEntrada,String opcionColumna)
    {
        int cuenta= 0;
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,"%" + textoEntrada + "%");
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                cuenta = resultado.getInt("CUENTA");
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
    public static List<OHSASTipoPrograma> listaTipoLike(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String textoEntrada,int desde,int hasta,String columna,String orden,String opcionColumna)
    {
        List<OHSASTipoPrograma> listaTipoPrograma = new ArrayList<OHSASTipoPrograma>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);

            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setInt(2,desde);
            pstmt.setInt(3,hasta);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASTipoPrograma p = new OHSASTipoPrograma();
                p.setIdTipoPrograma(resultado.getString("ID_TIPO_PROGRAMA"));
                p.setDescripcion(resultado.getString("DESCRIPCION"));
                listaTipoPrograma.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR: " +e.toString());
        }
        return listaTipoPrograma;
    }
    
    /*Obtiene la lista de tipo de programa*/
    public static List<OHSASTipoPrograma> listaTipo(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa)
    {
        List<OHSASTipoPrograma> listaTipoPrograma = new ArrayList<OHSASTipoPrograma>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASTipoPrograma p = new OHSASTipoPrograma();
                p.setIdTipoPrograma(resultado.getString("ID_TIPO_PROGRAMA"));
                p.setDescripcion(resultado.getString("DESCRIPCION"));
                listaTipoPrograma.add(p);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR: " +e.toString());
        }
        return listaTipoPrograma;
    }
    
    /*Actualiza los datos de un tipo de programa*/
    public static void update(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String codigoTipo,
        String descripcionTipo,String dirigido,String revision,List<String> observacion) throws SQLException
    {
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
        conn.setAutoCommit(false);
        try
        {
            String insertaTipo = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(insertaTipo);
            pstmt.setString(1,descripcionTipo);
            pstmt.setString(2,dirigido);
            pstmt.setString(3,revision);
            pstmt.setString(4,codigoEmpresa);
            pstmt.setString(5,codigoTipo);
            pstmt.executeUpdate();
            pstmt.close();
            
            String eliminaObservaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            pstmt = conn.prepareStatement(eliminaObservaciones);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoTipo);
            pstmt.executeUpdate();
            pstmt.close();
            for (String o : observacion)
            {
                String insertaObservacion = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(insertaObservacion);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,codigoTipo);
                pstmt.setString(3,codigoEmpresa);
                pstmt.setString(4,codigoTipo);
                pstmt.setString(5,o);
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
    /*Elimina un tipo de programa*/
    public static void delete(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String codigoTipo) 
        throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            conn.setAutoCommit(false);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoTipo);
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
    /*Obtiene un tipo de programa dado su codigo*/
    public static OHSASTipoPrograma getTipo(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String idTipo)
    {
        OHSASTipoPrograma tipo= new OHSASTipoPrograma();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idTipo);
            ResultSet resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                tipo.setIdTipoPrograma(resultado.getString("ID_TIPO_PROGRAMA"));
                tipo.setDescripcion(resultado.getString("DESCRIPCION"));
                tipo.setDirigido(resultado.getString("DIRIGIDO"));
                tipo.setRevision(resultado.getString("REVISION"));
            }
            pstmt.close();
            resultado.close();
            /*parte que obtiene las observaciones*/
            List<String> observaciones = new ArrayList<String>();
            
            String obtieneObservaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            pstmt = conn.prepareStatement(obtieneObservaciones);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idTipo);
            resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                observaciones.add(resultado.getString("OBSERVACION"));
            }
            tipo.setObservaciones(observaciones);
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return tipo;
    }
}
