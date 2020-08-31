package app

object singletonFunc {
    def removeSymbols(s: String): String ={
        s.replaceAll("[^a-zA-Z]","")
    }
}