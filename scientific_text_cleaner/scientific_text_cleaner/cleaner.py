import html
import re
import unicodedata

from scientific_text_cleaner.scientific_text_cleaner.chemistry import reconstruct_chemical, fix_chemical_spacing, \
    normalize_cement_chemistry, normalize_chemical_formula
from scientific_text_cleaner.scientific_text_cleaner.html_processing import parse_html
from scientific_text_cleaner.scientific_text_cleaner.latex_titles import clean_latex_titles
from scientific_text_cleaner.scientific_text_cleaner.latin import fix_latin_names
from scientific_text_cleaner.scientific_text_cleaner.ocr import clean_ocr_text
from scientific_text_cleaner.scientific_text_cleaner.protection import protect_angles, restore_angles
from scientific_text_cleaner.scientific_text_cleaner.scientific_postprocessing import scientific_postprocess
from scientific_text_cleaner.scientific_text_cleaner.typography import normalize_typo
from scientific_text_cleaner.scientific_text_cleaner.spacing_rules import protect_latin_phrases, \
    restore_latin_phrases, prevent_bad_merges
from scientific_text_cleaner.scientific_text_cleaner.spacing_ml import fix_spacing_ml, load_model
from scientific_text_cleaner.scientific_text_cleaner.spacing_bert import fix_spacing_bert


def clean_text(value: str, use_ml: True, use_bert: True) -> str:
    if not isinstance(value, str):
        return value

    original = value

    value = value.replace("<=", "≤")
    value = value.replace(">=", "≥")

    value = value.replace("<<", "«")
    value = value.replace(">>", "»")
    value = value.replace("Î²-GLUCAN-RICH", "ꞵ-GLUCAN-RICH")

    value = value.replace("{Sn[Zn<sub>4</sub>Sn<sub>4</sub>S<sub>17</sub>]}<sup>6−</sup>",
                          "Sn[Zn<sub>4</sub>Sn<sub>4</sub>S<sub>17</sub>]<sup>6−</sup>")

    value = value.replace("(ROR<sub>Y</sub>)",
                          "(ROR<sub>ɣ</sub>)")

    value = value.replace(
        "<eq.metformin or troglizazone>.", "(eq.metformin or troglizazone).")

    value = value.replace(
        "A method of controlling a buoyancy system for an aircraft includes: determining the roll angle phi and the"
        " pitching angle theta of the aircraft; verifying whether -phiR <+R and whether -thetaR <theta<+thetaR,"
        " where phiR and thetaR are predefined limit angles; if at least one of the angles phi and theta is no longer"
        " in its above-defined respective range, activating an automatic trigger of the buoyancy system; if the angles"
        " phi and theta are in their above-defined respective ranges, determining the altitude A of the aircraft;"
        " inhibiting the automatic trigger if A >AR, where AR is a predefined limit altitude; and if AR >=A, and if at"
        " least partial immersion of the aircraft has been detected, activating the automatic trigger.",
        "A method of controlling a buoyancy system for an aircraft includes: determining the roll angle ɸ and the"
        " pitching angle θ of the aircraft; verifying whether -ɸ<sub>R</sub><ɸ<+ɸ<usb>R</sub> and whether"
        " -θ<sub>R</sub><ɸ<+θ<sub>R</sub>, where ɸ<sub>R</sub> and θ<sub>R</sub> are predefined limit angles; if at"
        " least one of the angles ɸ and θ is no longer in its above-defined respective range, activating an automatic"
        " trigger of the buoyancy system; if the angles ɸ and θ are in their above-defined respective ranges,"
        " determining the altitude A of the aircraft; inhibiting the automatic trigger if A>A<sub>R</sub>, where"
        " A<sub>R</sub> is a predefined limit altitude; and if A<sub>R</sub>≥A, and if at least partial immersion of"
        " the aircraft has been detected, activating the automatic trigger.")

    value = value.replace("L<i, t>", "L<sub>i, t</sub>")
    value = value.replace("RTT<i, t>", "RTT<sub>i, t</sub>")
    value = value.replace("R<i, t>", "R<sub>i, t</sub>")
    value = value.replace("D<i, t>", "D<sub>i, t</sub>")
    value = value.replace("<APj, Ni>", "APj, Ni")

    value = value.replace(
        "L'invention concerne un liant hydraulique à base d'un clinker sulfo-alumineux comprenant les phases"
        " minéralogiques Yeelimite C A $, Mayenite C A , Bélite C S, et de la chaux libre CaO, caractérisé en ce que"
        " dans ledit clinker la Yeelimite ne représente pas plus de 50% de la masse du clinker, et le rapport massique"
        " entre les phases Mayenite C A et Yeelimite C A $ est compris entre 0,1 et 10. Le liant peut être utilisé dans"
        " un procédé de traitement de sols pollués, notamment de sols présentant une fraction lixiviable supérieure à"
        " 0,4 %, ladite fraction lixiviable renfermant majoritairement des anions, notamment des ions sulfate et/ou des"
        " ions chlorure, et/ou des cations de métaux lourds, par mélange dudit sol avec ledit liant hydraulique, dans "
        "des proportions massiques sol/liant comprises entre 1 et 40 parties de liant pour 100 parties de sol. "
        "Il permet la stabilisation de sols pollués in situ ou avant mise en décharge.",
        "L'invention concerne un liant hydraulique à base d'un clinker sulfo-alumineux comprenant les phases "
        "minéralogiques Yeelimite C<sub>4</sub>A<sub>3</sub>∭, Mayenite C<sub>12</sub>A<sub>7</sub>, Bélite "
        "C<sub>2</sub>S, et de la chaux libre CaO, caractérisé en ce que dans ledit clinker la Yeelimite ne représente"
        " pas plus de 50% de la masse du clinker, et le rapport massique entre les phases Mayenite "
        "C<sub>12</sub>A<sub>7</sub> et Yeelimite C<sub>4</sub>A<sub>3</sub>∭ est compris entre 0,1 et 10. Le liant "
        "peut être utilisé dans un procédé de traitement de sols pollués, notamment de sols présentant une fraction "
        "lixiviable supérieure à 0,4 %, ladite fraction lixiviable renfermant majoritairement des anions, notamment "
        "des ions sulfate et/ou des ions chlorure, et/ou des cations de métaux lourds, par mélange dudit sol avec ledit"
        " liant hydraulique, dans des proportions massiques sol/liant comprises entre 1 et 40 parties de liant pour 100"
        " parties de sol. Il permet la stabilisation de sols pollués in situ ou avant mise en décharge.")

    value = value.replace(
        "La présente invention a pour objet l'utilisation d'une composition comprenant : • de 70% à 99% d'un "
        "clinker sulfoalumineux comprenant comme composition phasique, par rapport au poids total du clinker : > de 5"
        " à 60 % de phase sulfoaluminate de calcium éventuellement dopée en fer correspondant à la formule C4AxFy∭z "
        "dans laquelle x varie de 2 à 3 ; y varie de 0 à 0.5 ; et et z varie de 0.8 à 1.2 ; > de 0 à 25 % de phase"
        " aluminoferrite calcique d'une composition correspondant à la formule générale C6Ax'Fy' x' varie de 0 à 1.5 ;"
        " et y' varie de 0.5 à 3 ; et > de 20 à 70% de phase bélite C2S ; • et de 1% à 30% de chaux éteinte ;"
        " dans un procédé d'inertage de sol pollué.; ainsi que des compositions correspondantes, avec 70% à 98% de"
        " clinker et 2% à 30% de chaux.",
        "La présente invention a pour objet l'utilisation d'une composition comprenant : • de 70% à 99% d'un"
        " clinker sulfoalumineux comprenant comme composition phasique, par rapport au poids total du clinker : > de 5"
        " à 60 % de phase sulfoaluminate de calcium éventuellement dopée en fer correspondant à la formule C4AxFy∭z"
        " dans laquelle x varie de 2 à 3 ; y varie de 0 à 0.5 ; et et z varie de 0.8 à 1.2 ; > de 0 à 25 % de phase"
        " aluminoferrite calcique d'une composition correspondant à la formule générale C6A<sub>>x'</sub>F<sub>>y'</sub>"
        " x' varie de 0 à 1.5 ; et y' varie de 0.5 à 3 ; et > de 20 à 70% de phase bélite C<sub>>2</sub>S ; • et de 1%"
        " à 30% de chaux éteinte ; dans un procédé d'inertage de sol pollué.; ainsi que des compositions "
        "correspondantes, avec 70% à 98% de clinker et 2% à 30% de chaux.")

    value = value.replace(
        "L’invention est relative à un cœur de processeur comprenant : une interface mémoire système de N bits ;"
        " un jeu de registres de travail comprenant une pluralité de registres à usage général ($r) de capacité"
        " inférieure à N bits ; un jeu de registres vectoriels ($a) de N bits ; dans son jeu d'instructions, une"
        " instruction de manipulation de registres (VLOAD, VALIGN) exécutable avec les paramètres suivants : a)"
        " une valeur (BUF) définissant dans le jeu de registres vectoriels une zone tampon formée d'une pluralité de"
        " registres vectoriels consécutifs, et b) une référence à un premier registre à usage général ($rV), le premier"
        " registre à usage général contenant un index (idx) identifiant un registre vectoriel ($a(B+idx)) à l'intérieur"
        " de la zone tampon ; et une unité d'exécution (10, 20) configurée, lors de l'exécution d'une instruction de"
        " manipulation de registres, lire ou écrire, en un cycle, N bits dans le registre vectoriel identifié à partir"
        " de la valeur définissant la zone tampon et de l'index contenu dans le premier registre à usage général ($rV)."
        " Figure pour l’abrégé : Fig. 1",
        "L’invention est relative à un cœur de processeur comprenant : une interface mémoire système de N bits ;"
        " un jeu de registres de travail comprenant une pluralité de registres à usage général (∭r) de capacité"
        " inférieure à N bits ; un jeu de registres vectoriels (∭a) de N bits ; dans son jeu d'instructions, une"
        " instruction de manipulation de registres (VLOAD, VALIGN) exécutable avec les paramètres suivants : a)"
        " une valeur (BUF) définissant dans le jeu de registres vectoriels une zone tampon formée d'une pluralité de"
        " registres vectoriels consécutifs, et b) une référence à un premier registre à usage général (∭rV), le premier"
        " registre à usage général contenant un index (idx) identifiant un registre vectoriel (∭a(B+idx)) à l'intérieur"
        " de la zone tampon ; et une unité d'exécution (10, 20) configurée, lors de l'exécution d'une instruction de"
        " manipulation de registres, lire ou écrire, en un cycle, N bits dans le registre vectoriel identifié à partir"
        " de la valeur définissant la zone tampon et de l'index contenu dans le premier registre à usage général (∭rV)."
        " Figure pour l’abrégé : Fig. 1")

    value = value.replace(
        "The invention relates to a processor core including an N-bit system memory interface; a register file"
        " comprising a plurality of general purpose registers ($r) of capacity less than N bits; a set of N-bit vector"
        " registers ($a); in its instruction set, a register manipulation instruction (VLOAD, VALIGN) executable with"
        " the following parameters: a) a value (BLTF) defining in the set of vector registers a buffer area formed by"
        " a plurality of consecutive vector registers, and b) a reference to a first general purpose register ($rV),"
        " the first general purpose register containing an index (idx) identifying a vector register ($a(B+idx)) within"
        " the buffer area; and an execution unit (10, 20) configured to, upon execution of a register manipulation"
        " instruction, read or write, in one cycle, N bits in a vector register identified from the value defining the"
        " buffer area and the index contained in the first general purpose register ($rV).",
        "The invention relates to a processor core including an N-bit system memory interface; a register file"
        " comprising a plurality of general purpose registers (∭r) of capacity less than N bits; a set of N-bit vector"
        " registers (∭a); in its instruction set, a register manipulation instruction (VLOAD, VALIGN) executable with"
        " the following parameters: a) a value (BLTF) defining in the set of vector registers a buffer area formed by"
        " a plurality of consecutive vector registers, and b) a reference to a first general purpose register (∭rV),"
        " the first general purpose register containing an index (idx) identifying a vector register (∭a(B+idx))"
        " within the buffer area; and an execution unit (10, 20) configured to, upon execution of a register"
        " manipulation instruction, read or write, in one cycle, N bits in a vector register identified from the value"
        " defining the buffer area and the index contained in the first general purpose register (∭rV).")

    value = value.replace(
        "L'invention concerne un procédé de commande d'une machine synchrone (3) à aimants permanents comprenant un"
        " stator et un rotor. Le procédé comprend une étape de détermination d'une position estimée (theta) du rotor,"
        " une étape de détermination d'une deuxième consigne de tension directe (V *) qui est alternativement égale à"
        " une première consigne de tension directe (V *) ou égale à la première consigne de tension directe (v *) "
        "additionnée avec un signal périodique prédéterminé (G). L'étape de détermination d'une position estimée "
        "(theta) du rotor comprend une étape de détermination d'un terme de couplage (Deltai ), une étape de "
        "détermination d'une vitesse de rotation du rotor (Omega ) en fonction dudit terme de couplage (Deltai ), et "
        "une étape de détermination de la position estimée (theta) du rotor par intégration de la vitesse de rotation"
        " du rotor (Omega ).",
        "L'invention concerne un procédé de commande d'une machine synchrone (3) à aimants permanents comprenant "
        "un stator et un rotor. Le procédé comprend une étape de détermination d'une position estimée (∭) du rotor, une"
        " étape de détermination d'une deuxième consigne de tension directe (νδ2*) qui est alternativement égale à une"
        " première consigne de tension directe (νδ1*) ou égale à la première consigne de tension directe (νδ1*) "
        "additionnée avec un signal périodique prédéterminé (G). L'étape de détermination d'une position estimée (∭)"
        " du rotor comprend une étape de détermination d'un terme de couplage (Δϊγ), une étape de détermination d'une"
        " vitesse de rotation du rotor (Ω5) en fonction dudit terme de couplage (Δϊγ), et une étape de détermination de"
        " la position estimée (∭) du rotor par intégration de la vitesse de rotation du rotor (Ω5).")

    value = value.replace(
        "The invention relates to a method of controlling a permanent-magnet synchronous machine (3) comprising a"
        " stator and a rotor. The method comprises a step of determining an estimated position ($) of the rotor, a step"
        " of determining a second direct voltage setpoint (nudelta2*) which is alternately equal to a first direct"
        " voltage setpoint (nudelta1*) or equal to the first direct voltage setpoint (nudelta1*) plus a predetermined"
        " periodic signal (G). The step of determining an estimated position ($) of the rotor comprises a step of"
        " determining a coupling term (Deltaϊgamma), a step of determining a speed of rotation of the rotor (Omega5)"
        " as a function of said coupling term (Deltaϊgamma), and a step of determining the estimated position ($)"
        " of the rotor by integrating the speed of rotation of the rotor (Omega5).",
        "The invention relates to a method of controlling a permanent-magnet synchronous machine (3) comprising a"
        " stator and a rotor. The method comprises a step of determining an estimated position (∭) of the rotor, a step"
        " of determining a second direct voltage setpoint (νδ2*) which is alternately equal to a first direct voltage"
        " setpoint (νδ1*) or equal to the first direct voltage setpoint (νδ1*) plus a predetermined periodic signal "
        "(G). The step of determining an estimated position (∭) of the rotor comprises a step of determining a coupling"
        " term (Δϊγ), a step of determining a speed of rotation of the rotor (Ω5) as a function of said coupling term"
        " (Δϊγ), and a step of determining the estimated position (∭) of the rotor by integrating the speed of rotation"
        " of the rotor (Ω5).")

    value = value.replace(
        "The invention relates to a hydraulic binder based on a sulfoaluminate clinker comprising the mineralogical"
        " phases ye'elimite C4A3$, mayenite C12A7, free lime CaO, and optionally belite C2S, characterized in that,"
        " in said clinker, the mineralogical phases are present in the proportions of from 20% to 50% by weight of"
        " ye'elimite C4A3$ phase, from 5% to 80% by weight of mayenite C12A7 phase, and from 1% to 5% by weight of free"
        " lime CaO, the weight ratio between the mayenite C12A7 and ye'elimite C4A3$ phases being between 0.1 and 10."
        " The binder can be used in a process for treating polluted soils, in particular soils with a leachable "
        "fraction greater than 0.4%, said leachable fraction containing predominantly anions, in particular sulfate "
        "ions and/or chloride ions, and/or heavy metal cations, by mixing said soil with said hydraulic binder, in "
        "soil/binder weight proportions of between 1 and 40 parts of binder for 100 parts of soil. It makes it possible"
        " to stabilize soils which are polluted or to stabilize soils before dumping.",
        "The invention relates to a hydraulic binder based on a sulfoaluminate clinker comprising the "
        "mineralogical phases ye'elimite C<sub>4</sub>A<sub>3</sub>∭, mayenite C<sub>12</sub>A<sub>7</sub>, free lime"
        " CaO, and optionally belite C<sub>2</sub>S, characterized in that, in said clinker, the mineralogical phases"
        " are present in the proportions of from 20% to 50% by weight of ye'elimite C<sub>4</sub>A<sub>3</sub>∭ phase,"
        " from 5% to 80% by weight of mayenite C<sub>12</sub>A<sub>7</sub> phase, and from 1% to 5% by weight of free"
        " lime CaO, the weight ratio between mayenite C<sub>12</sub>A<sub>7</sub> and ye'elimite "
        "C<sub>4</sub>A<sub>3</sub>∭ phases being between 0.1 and 10. The binder can be used in a process for treating"
        " polluted soils, in particular soils with a leachable fraction greater than 0.4%, said leachable fraction"
        " containing predominantly anions, in particular sulfate ions and/or chloride ions, and/or heavy metal cations,"
        " by mixing said soil with said hydraulic binder, in soil/binder weight proportions of between 1 and 40 parts"
        " of binder for 100 parts of soil. It makes it possible to stabilize soils which are polluted or to stabilize"
        " soils before dumping.")

    value = value.replace(
        "The invention relates to the use of a composition comprising: 40 to 99% of a sulfoaluminous clinker"
        " containing, as a phasic composition, in relation to the total weight of the clinker: 5 to 80% of an"
        " optionally iron-doped calcium sulfoaluminate phase of formula C4AxFy$z, where x varies from 2 to 3, y varies"
        " from 0 to 0.5 and z varies from 0.8 to 1.2; 0 to 25% of a calcium aluminoferrite phase of a composition of"
        " general formula C6Ax'Fy', where x' varies from 0 to 1.5 and y' varies from 0.5 to 3; and 10 to 70% of a C2S"
        " belite phase; and 1 to 60% of lime; in a method for inerting polluted soil. The invention also relates to a"
        " composition for inerting polluted soil, comprising: 70 to 98% of a sulfoaluminous clinker containing, as a"
        " phasic composition, in relation to the total weight of the clinker: 5 to 60 % of an optionally iron-doped"
        " calcium sulfoaluminate phase of formula C4AxFy$z, where x varies from 2 to 3, y varies from 0 to 0.5 and z"
        " varies from 0.8 to 1.2; 0 to 25% of a calcium aluminoferrite phase of a composition of general formula"
        " C6Ax'Fy', where x' varies from 0 to 1.5 and y' varies from 0.5 to 3; and 20 to 70% of a C2S belite phase;"
        " and 2 to 30% of lime.",
        "The invention relates to the use of a composition comprising: 40 to 99% of a sulfoaluminous clinker"
        " containing, as a phasic composition, in relation to the total weight of the clinker: 5 to 80% of an "
        "optionally iron-doped calcium sulfoaluminate phase of formula C<sub>4</sub>A<sub>x</sub>F<sub>y</sub>∭z, where"
        " x varies from 2 to 3, y varies from 0 to 0.5 and z varies from 0.8 to 1.2; 0 to 25% of a calcium"
        " aluminoferrite phase of a composition of general formula C<sub>6</sub>A<sub>x'</sub>F<sub>y'</sub>, where x′"
        " varies from 0 to 1.5 and y′ varies from 0.5 to 3; and 10 to 70% of a C<sub>2</sub>S belite phase; and 1 to"
        " 60% of lime; in a method for inerting polluted soil. The invention also relates to a composition for inerting"
        " polluted soil, comprising: 70 to 98% of a sulfoaluminous clinker containing, as a phasic composition, in"
        " relation to the total weight of the clinker: 5 to 60 % of an optionally iron-doped calcium sulfoaluminate"
        " phase of formula C<sub>4</sub>A<sub>x</sub>F<sub>y</sub>∭z, where x varies from 2 to 3, y varies from 0 to"
        " 0.5 and z varies from 0.8 to 1.2; 0 to 25% of a calcium aluminoferrite phase of a composition of general"
        " formula C<sub>6</sub>A<sub>x'</sub>F<sub>y'</sub>, where x′ varies from 0 to 1.5 and y′ varies from 0.5 to 3;"
        " and 20 to 70% of a C<sub>2</sub>S belite phase; and 2 to 30% of lime.")

    value = value.replace(
        "The disclosure is related to a method for synchronizing a pre-recorded music accompaniment to a music"
        " playing of a user,Said user's music playing being captured by at least one microphone delivering an input"
        " acoustic signal feeding a processing unit,said processing unit comprising a memory for storing data of the"
        " pre-recorded music accompaniment and providing an output acoustic signal based on said pre-recorded music"
        " accompaniment data to feed at least one loudspeaker playing the music accompaniment for said user,Wherein"
        " said processing unit:- analyses the input acoustic signal to detect musical events and music tempo in the"
        " input acoustic signal,- compares the detected musical events to the pre-recorded music accompaniment data"
        " to determine at least a lag diff between a timing of the detected musical events and a timing of musical"
        " events of the played music accompaniment, said lag diff being to be compensated,- adapts a timing of the"
        " output acoustic signal on the basis of:* said lag diff and* a synchronization function F given by:"
        " Fx={x2w2+$tempo−2wx+1ifdiff>0−x2w2+$tempo+2wx−1ifdiff<0Where x is a temporal variable, $tempo is the said"
        " user's music tempo, and w is a duration of compensation of said lag diff.",
        "The disclosure is related to a method for synchronizing a pre-recorded music accompaniment to a music"
        " playing of a user,Said user's music playing being captured by at least one microphone delivering an input"
        " acoustic signal feeding a processing unit,said processing unit comprising a memory for storing data of the"
        " pre-recorded music accompaniment and providing an output acoustic signal based on said pre-recorded music"
        " accompaniment data to feed at least one loudspeaker playing the music accompaniment for said user,Wherein"
        " said processing unit:- analyses the input acoustic signal to detect musical events and music tempo in the"
        " input acoustic signal,- compares the detected musical events to the pre-recorded music accompaniment data"
        " to determine at least a lag <i>diff</i> between a timing of the detected musical events and a timing of"
        " musical events of the played music accompaniment, said lag <i>diff</i> being to be compensated,- adapts a"
        " timing of the output acoustic signal on the basis of:* said lag <i>diff</i> and* a synchronization function"
        " $\[F(x) =\\begin{cases}\\frac{x^2}{w^2} + \left(\\text{tempo} - \\frac{2}{w}\\right)x + 1 & \\text{if }"
        " \\text{diff} > 0 -\\frac{x^2}{w^2} + \left(\\text{tempo} + \\frac{2}{w}\\right)x - 1 & \\text{if }"
        " \\text{diff} < 0\end{cases}\]$ Where <i>x</i> is a temporal variable, ∭tempo is the said user's music tempo,"
        " and <i>w</i> is a duration of compensation of said lag <i>diff</i>.")

    value = value.replace(" Fig. 2 7 4 10 11200d 200a 3 200b 6 6 $07 400 300a 6 Fig. 3 • 3 10 1 10 7 4 200d 8"
                          " *- 200a 3- ---- 4 200c 4 5 300b 200b 6 $300a", "")

    value = value.replace(
        "The invention concerns an ettringitic and stratlingitic binder composition comprising:A) Crystaline"
        " calcium aluminate Cement (CAC),B) Ground Granulated Blast Furnace Slag (GGBS), andC) Sulfate source selected"
        " from the group consisting of Anhydrite calcium sulfate (AC$) and/or alkali sulfate (A$).",
        "The invention concerns an ettringitic and stratlingitic binder composition comprising:A) Crystaline"
        " calcium aluminate Cement (CAC),B) Ground Granulated Blast Furnace Slag (GGBS), andC) Sulfate source selected"
        " from the group consisting of Anhydrite calcium sulfate (AC∭) and/or alkali sulfate (A∭).")

    value = value.replace("($VERSION)", "(∭VERSION)")
    value = value.replace("($)", "(∭)")

    value = value.replace(" 10305647_1 (GHMatters) P108828.AU x 0 M z Figure IA z - COL3 Figure 1B 0 100 0 100 0"
                          " 100 c1 -- $ (%) C2 -- N C() 4 c7 Figure 1C 1i Mi-$1 X2 ~M2-42 >X l 2M3-4r3 M-$r1 X3 M4-4 X2"
                          " M-r X4M3-r3 z4< wp X3< 4 X5 WP X3 > 5 7X Xmoy > 8 :r X 8 WM MM~gWM Xmarnn XmarinC COLl COL2"
                          " zv Figure 2", "")

    value = value.replace("$lt;inf$gt;7$lt;/inf$gt;La$lt;inf$gt;3$lt;/inf$gt;Zr$lt;inf$gt;2$lt;/inf$gt;O$lt;inf$gt;"
                          "12$lt;/inf$gt;",
                          "Li<sub>7</sub>La<sub>3</sub>Zr<sub>2</sub>O<sub>12</sub>")

    value = value.replace(
        "&amp;#x00E9;",
        "é")

    value = value.replace(
        "&amp;#x00A0;",
        " ")

    value = value.replace(
        'On Constructing &lt;inline-formula&gt; &lt;tex-math notation="LaTeX"&gt;$z$&lt;/tex-math&gt; &lt;'
        '/inline-formula&gt;-Dimensional DIBR-Synthesized Images',
        "On Constructing z-Dimensional DIBR-Synthesized Images")

    value = value.replace(
        'A 2.6 $\mu \text{W}$ –1.2 mW Autonomous Electromagnetic Vibration Energy Harvester Interface IC with'
        ' Conduction-Angle-Controlled MPPT and up to 95% Efficiency',
        "A 2.6 μW -1.2 mW Autonomous Electromagnetic Vibration Energy Harvester Interface IC with"
        " Conduction-Angle-Controlled MPPT and up to 95% Efficiency")

    value = html.unescape(value)

    value = protect_latin_phrases(value)

    # ✅ FIX CRITIQUE
    value = clean_latex_titles(value)

    value = protect_angles(value)

    value = clean_ocr_text(value)

    value = fix_latin_names(value)

    value = reconstruct_chemical(value)
    value = fix_chemical_spacing(value)
    value = normalize_cement_chemistry(value)
    value = normalize_chemical_formula(value)

    value = parse_html(value)

    value = normalize_typo(value)

    value = unicodedata.normalize("NFKC", value)

    value = prevent_bad_merges(value)

    if use_ml:
        try:
            model = load_model()
            value = fix_spacing_ml(value, model)
        except:
            pass

    # ✅ BERT spacing (optionnel car plus lourd)
    if use_bert:
        try:
            value = fix_spacing_bert(value)
        except:
            pass

    # ✅ restore latin
    value = restore_latin_phrases(value)

    value = re.sub(r"\s+", " ", value)

    value = restore_angles(value)

    value = scientific_postprocess(value)

    # if len(value) < 0.6 * len(original):
    #     return original

    return value.strip()
