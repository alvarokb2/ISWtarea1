#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <cstdlib>
#include <string.h>
#include <fstream>
#include <libpq-fe.h>

//constantes
#define LConsulta 500
using namespace std;

//variables globales
char Consulta[LConsulta];

PGconn *conexion = NULL;
PGresult *resultado_SQL = NULL;
const char *servidor = "146.83.181.4";
const char *puerto = "6432";
const char *esquema = "isw";
const char *baseDato = "iswdb";
const char *usuario = "estisw";
const char *contrasena = "estisw";

//imprime integrantes y fecha de compilacion
void ImprimirIntegrantes()
{
cout << "Integrantes del grupo de trabajo : " << endl << endl;
cout << " 1.- Alvaro Cabedo Vasquez" << endl;
cout << " 2.- Cristian Mondaca Ruiz" << endl;
cout << " 3.- Javier Reyes Gonzalez" << endl;
cout << " Fecha de compilacion : " << __DATE__ << endl;
}

//vaciar consulta
void VaciarConsulta() {
    for (int i = 0; i < LConsulta; i++) {
        Consulta[i] = '\0';
    }
}

//imprimir resultado
void ImprimirResultado(PGresult *resultado) {
    int filas = PQntuples(resultado);
    int columnas = PQnfields(resultado);
    for (int i = 0; i < filas; i++) {
        for (int j = 0; j < columnas; j++) {
            cout << PQgetvalue(resultado, i, j) << "; ";

        }
        cout << endl;
    }

}

//generar resultado
void setResultado() {
    conexion = PQsetdbLogin(servidor, puerto, NULL, NULL, baseDato, usuario, contrasena);
    if (PQstatus(conexion) != CONNECTION_BAD) {
        cout << "conectado a la BD\n";
        resultado_SQL = PQexec(conexion, Consulta);
        if (resultado_SQL != NULL) {
            cout << "consulta exitosa\n";
        } else
            cout << "error en la consulta";
    } else cout << "Hubo un error en la conexion a la base de datos" << endl;
    PQfinish(conexion);
    /*FIn de la conexion con la base de datos*/
}

//consulta cantidad de peticiones por estado agrupado por fecha
void setConsulta_g(char *fecha_ini, char *fecha_fin) {
    VaciarConsulta();
    const char *sql = "SELECT ";

    strcpy(Consulta, sql);
    strcat(Consulta, "CONCAT(EXTRACT (MONTH FROM fecha),'-',");
    strcat(Consulta, "EXTRACT (YEAR FROM fecha)) as mes, ");
    strcat(Consulta, "estado,");
    strcat(Consulta, "COUNT(peticion) as peticiones ");
    strcat(Consulta, "FROM isw.accesos ");
    strcat(Consulta, "WHERE ");
    strcat(Consulta, "fecha >= '");
    strcat(Consulta, fecha_ini);
    strcat(Consulta, " 00:00:00' AND fecha <= '");
    strcat(Consulta, fecha_fin);
    strcat(Consulta, " 23:59:59' GROUP BY mes , estado ");
    strcat(Consulta, "ORDER BY mes, peticiones, estado;");
}

//consulta porcentaje de peticiones por estado en el intervalo de fechas
void setConsulta_t(char *fecha_ini, char *fecha_fin) {
    VaciarConsulta();
    const char *sql = "SELECT ";
    strcpy(Consulta, sql);
    strcat(Consulta, "estado, ");
    strcat(Consulta, "(COUNT(peticion)*100 / (SELECT COUNT(peticion) ");
    strcat(Consulta, "FROM isw.accesos ");
    strcat(Consulta, "WHERE ");
    strcat(Consulta, "fecha >= '");
    strcat(Consulta, fecha_ini);
    strcat(Consulta, " 00:00:00' AND fecha <= '");
    strcat(Consulta, fecha_fin);
    strcat(Consulta, " 23:59:59'))");
    strcat(Consulta, "AS percent ");
    strcat(Consulta, "FROM isw.accesos ");
    strcat(Consulta, "WHERE ");
    strcat(Consulta, "fecha >= '");
    strcat(Consulta, fecha_ini);
    strcat(Consulta, " 00:00:00' AND fecha <= '");
    strcat(Consulta, fecha_fin);
    strcat(Consulta, " 23:59:59' GROUP BY estado ");
    strcat(Consulta, "ORDER BY percent, estado;");
}

//consulta 100 peticiones mas solicitadas en el intervalo de fechas
void setConsulta_a(char *fecha_ini, char *fecha_fin) {
    VaciarConsulta();
    const char *sql = "SELECT ";
    strcpy(Consulta, sql);
    strcat(Consulta, "peticion, ");
    strcat(Consulta, "COUNT(peticion) AS peticiones ");
    strcat(Consulta, "FROM isw.accesos ");
    strcat(Consulta, "WHERE ");
    strcat(Consulta, "fecha >= '");
    strcat(Consulta, fecha_ini);
    strcat(Consulta, " 00:00:00' AND fecha <= '");
    strcat(Consulta, fecha_fin);
    strcat(Consulta, " 23:59:59' GROUP BY peticion ");
    strcat(Consulta, "ORDER BY peticiones DESC ");
    strcat(Consulta, "LIMIT 100;");
}

//validacion fechas
bool ValidarFecha(string fecha) {
    if (fecha.length() == 10) { //valida largo fecha
        if (fecha.substr(4, 1) == "-" && fecha.substr(7, 1) == "-") { //valida guiones
            for (int i = 0; i < fecha.length(); i++) {
                if (i == 4 || i == 7) { //donde hay guiones salta
                    i++;

                }
                if (!isdigit(fecha[i])) { //valida que sean digitos
                    cout << "no digito\n";
                    return false;
                }
            }
            int anio = atoi(&fecha[0]);
            int mes = atoi(&fecha[5]);
            int dia = atoi(&fecha[8]);
            //meses con 31 dias
            if (mes == 1 || mes == 3 || mes == 5 || mes == 7 || mes == 8 || mes == 10 || mes == 12) {
                if (dia <= 31 && dia > 0) {
                    return true;
                } else {
                    cout << "dia no valido\n";
                }
            }//meses con 30 dias
            else if (mes == 4 || mes == 6 || mes == 9 || mes == 11) {
                if (dia <= 30 && dia > 0) {
                    return true;
                } else {
                    cout << "dia no valido\n";
                }
            }//febrero y anio bisciesto
            else if (mes == 2) {
                bool p = anio % 4 == 0;
                bool q = anio % 100 == 0;
                bool r = anio % 400 == 0;
                int dias;
                if ((p && !q) || r) {
                    dias = 29;
                } else {
                    dias = 28;
                }
                if (dia <= dias && dia > 0) {
                    return true;
                } else {
                    cout << "dia no valido\n";
                }
            } else {
                cout << "fecha invalida\n";
            }
                } else {
            cout << "guiones invalidos\n";
            return false;
        }
    } else {
        cout << "largo invalido\n";
        return false;
    }
}

//generar archivo csv
void generarCSV(PGresult *resultado){
    cout << "Generando 100peticiones.csv ..." << endl;
    ofstream fs ("100peticiones.csv");
    int filas = PQntuples(resultado);
    int columnas = PQnfields(resultado);
    for (int i = 0; i < filas; i++) {
        for (int j = 0; j < columnas; j++) {
            fs << "\"" << PQgetvalue(resultado, i, j) << "\";";
        }
        fs << endl;
    }
    fs.close();
}

//Punto de entrada
int main(int argc, char** argv) {
    cout << endl;
    switch (argc) {

        case 1://si no entran parametros
            cout << "Debe Ingresar Argumentos\n";
            break;

        case 2://si entra 1 parametro
            if (strcmp(*(argv + 1), "-v") == 0) { //si el parametro es "-v" muestra integrantes y fecha compilacion
                ImprimirIntegrantes();
            } else { //sino muestra mensaje error
                cout << "Argumento no valido\n";
            }
            break;

        case 4: //si se ingresan 3 parametros
            ValidarFecha(*(argv + 2));
            ValidarFecha(*(argv + 3));

            if (strcmp(*(argv + 1), "-g") == 0) { //si el primer parametro es "-g"
                cout << "Consultando cantidad de peticiones por estado, agrupados por fecha..."<<endl;
                setConsulta_g(*(argv + 2), *(argv + 3));
                setResultado();
                ImprimirResultado(resultado_SQL);
            } else
                if (strcmp(*(argv + 1), "-t") == 0) { //si el primer parametro es "-t"
                cout << "Consultando porcentaje de peticiones por estado..." << endl;
                setConsulta_t(*(argv + 2), *(argv + 3));
                setResultado();
                ImprimirResultado(resultado_SQL);
                } else
                if (strcmp(*(argv + 1), "-a") == 0) { //si el primer parametro es "-a"
                cout << "Consultando 100 peticiones mas solicitadas..." << endl;
                setConsulta_a(*(argv + 2), *(argv + 3));
                setResultado();
                generarCSV(resultado_SQL);
            } else { //si el primer parametro no es ninguno de los 3 anteriores
                cout << "Primer argumento invalido\n";
            }

            break;
        default:
            cout << "Numero de parametros invalido\n";
    }

    return 0;
}
